/*
 * Copyright (C) 2016-2022  Davidson Francis <davidsondfgl@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

/* clang-format off */
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
typedef int socklen_t;
#endif
/* clang-format on */

/* Windows and macOS seems to not have MSG_NOSIGNAL */
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* Windows does not have MSG_DONTWAIT (the lossy path uses blocking sends
 * there, still bounded by WS_LOSSY_STALL_MS through select()). */
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif

#include <unistd.h>

#include <utf8.h>
#include <ws.h>

/**
 * @dir src/
 * @brief wsServer source code
 *
 * @file ws.c
 * @brief wsServer main routines.
 */

/*
 * Lossy (non-blocking) output path, see ws_sendframe_lossy().
 *
 * Clients that use it own an outbox: at most two frames (the one the sender
 * thread is writing plus the newest one not started yet) and a sender thread
 * that pushes them to the socket. Producers only copy the frame into the
 * outbox - bounded work, never waiting on the network - so one slow client
 * cannot stall whoever produces the frames. Whoever falls behind is reported
 * back to the producer so it can, e.g., emit a key frame.
 */

/**
 * @brief Written into ws_outbox.magic while its locks exist. Both values are
 * reachable only through ws_outbox_prepare()/ws_outbox_teardown(), and the
 * client array is memset(-1) at startup, so slot reuse can never mistake
 * uninitialised memory for a live outbox.
 */
#define WS_OUTBOX_MAGIC 0x58424F55u
#define WS_OUTBOX_IDLE 0x444C49u

/**
 * @brief Writability poll granularity while a frame is being written. A client
 * that refuses data for WS_LOSSY_STALL_MS straight gets disconnected.
 */
#define WS_LOSSY_STALL_SLICE_MS 200

/**
 * @brief One frame held by an outbox: header and payload in a single buffer so
 * the sender writes it with as few syscalls as possible.
 */
struct ws_outframe
{
	unsigned char *buf; /**< Frame header + payload.                 */
	size_t alloc;       /**< Capacity of buf.                        */
	size_t len;         /**< Bytes to write.                         */
	size_t sent;        /**< Bytes already accepted by the socket.   */
};

/**
 * @brief Outbox of one client, see above.
 *
 * 'wslot' is the slot producers fill; whenever the sender picks that frame up
 * it flips to the other slot, which is why the sender always writes a slot no
 * producer can touch and needs no lock while writing.
 */
struct ws_outbox
{
	uint32_t magic;        /**< WS_OUTBOX_MAGIC while usable.          */
	pthread_mutex_t mtx;   /**< Guards everything below.               */
	pthread_cond_t cnd;    /**< Signalled when a frame is queued.      */
	pthread_t thread;      /**< Sender thread.                         */
	bool started;          /**< Sender thread was created.             */
	bool quit;             /**< Sender thread must stop.               */
	bool dead;             /**< Socket unusable: stop queueing.        */
	bool pending;          /**< slots[wslot] holds a queued frame.     */
	unsigned wslot;        /**< Slot producers fill, 0 or 1.           */
	int fd;                /**< Own dup of the socket, sender-owned.   */
	struct ws_outframe slots[WS_LOSSY_QUEUE_DEPTH];
};

/* Lossy outbox routines, see ws_sendframe_lossy(). */
static int ws_outbox_prepare(ws_cli_conn_t *client);
static int ws_outbox_write(struct ws_outbox *ob, struct ws_outframe *frame);
static void *ws_outbox_sender(void *arg);
static void ws_outbox_teardown(ws_cli_conn_t *client);
static int ws_outbox_enqueue(ws_cli_conn_t *client, const char *msg, size_t size,
	int type, int *behind);

/**
 * @brief Guards the client table for the lossy send path.
 *
 * A separate lock on purpose: the legacy blocking path (ws_sendframe, ws_ping)
 * holds the global mutex while it waits on send(), and the whole point of the
 * lossy path is that its callers never wait behind that. The lossy mutex is
 * only ever held around a table scan plus a memcpy, never around I/O.
 */
static pthread_mutex_t lossy_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Websocket events.
 */
static struct ws_events cli_events;

/**
 * @brief Client socks.
 */
struct ws_connection
{
	int client_sock; /**< Client socket FD.        */
	int state;       /**< WebSocket current state. */

	/* Timeout thread and locks. */
	pthread_mutex_t mtx_state;
	pthread_cond_t cnd_state_close;
	pthread_t thrd_tout;
	bool close_thrd;

	/* Send lock. */
	pthread_mutex_t mtx_snd;

	/* Lossy (non-blocking) outbox and its sender thread. */
	struct ws_outbox outbox;

	/* IP address. */
	char ip[INET6_ADDRSTRLEN];

	/* Ping/Pong IDs and locks. */
	int32_t last_pong_id;
	int32_t current_ping_id;
	pthread_mutex_t mtx_ping;
};

/**
 * @brief Clients list.
 */
static struct ws_connection client_socks[MAX_CLIENTS];

/**
 * @brief Timeout to a single send().
 */
static uint32_t timeout;

/**
 * @brief Reads/writes the socket fd of a client.
 *
 * The fd is published by the accept loop and reset by close_client() while
 * they hold the global mutex plus the lossy mutex, but a lot of code reads it
 * with no lock at all (validity checks, send/recv, debug prints). Going
 * through atomics keeps those readers from ever seeing a torn value and,
 * together with the locking of every write, from using an fd number that was
 * just closed and may already have been reopened for something else.
 */
static inline int cli_fd(ws_cli_conn_t *client)
{
	return (WS_CLI_FD(client));
}

static inline void cli_fd_set(ws_cli_conn_t *client, int fd)
{
	__atomic_store_n(&client->client_sock, fd, __ATOMIC_RELEASE);
}

/**
 * @brief Client validity macro
 */
#define CLIENT_VALID(cli)                          \
	((cli) != NULL && (cli) >= &client_socks[0] && \
		(cli) <= &client_socks[MAX_CLIENTS - 1] && \
		cli_fd(cli) > -1)

/**
 * @brief WebSocket frame data
 */
struct ws_frame_data
{
	/**
	 * @brief Frame read.
	 */
	unsigned char frm[MESSAGE_LENGTH];
	/**
	 * @brief Processed message at the moment.
	 */
	unsigned char *msg;
	/**
	 * @brief Control frame payload
	 */
	unsigned char msg_ctrl[125];
	/**
	 * @brief Current byte position.
	 */
	size_t cur_pos;
	/**
	 * @brief Amount of read bytes.
	 */
	size_t amt_read;
	/**
	 * @brief Frame type, like text or binary.
	 */
	int frame_type;
	/**
	 * @brief Frame size.
	 */
	uint64_t frame_size;
	/**
	 * @brief Error flag, set when a read was not possible.
	 */
	int error;
	/**
	 * @brief Client connection structure.
	 */
	ws_cli_conn_t *client;
};

/**
 * @brief Global mutex.
 */
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Issues an error message and aborts the program.
 *
 * @param s Error message.
 */
#define panic(s)   \
	do             \
	{              \
		perror(s); \
		exit(-1);  \
	} while (0);

/**
 * @brief Shutdown and close a given socket.
 *
 * @param fd Socket file descriptor to be closed.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void close_socket(int fd)
{
#ifndef _WIN32
	shutdown(fd, SHUT_RDWR);
	close(fd);
#else
	closesocket(fd);
#endif
}

/**
 * @brief Returns the current client state for a given
 * client @p client.
 *
 * @param client Client structure.
 *
 * @return Returns the client state, -1 otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int get_client_state(ws_cli_conn_t *client)
{
	int state;

	if (!CLIENT_VALID(client))
		return (-1);

	pthread_mutex_lock(&client->mtx_state);
	state = client->state;
	pthread_mutex_unlock(&client->mtx_state);
	return (state);
}

/**
 * @brief Set a state @p state to the client index
 * @p client.
 *
 * @param client Client structure.
 * @param state State to be set.
 *
 * @return Returns 0 if success, -1 otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int set_client_state(ws_cli_conn_t *client, int state)
{
	if (!CLIENT_VALID(client))
		return (-1);

	if (state < 0 || state > 3)
		return (-1);

	pthread_mutex_lock(&client->mtx_state);
	client->state = state;
	pthread_mutex_unlock(&client->mtx_state);
	return (0);
}

/**
 * @brief Send a given message @p buf on a socket @p sockfd.
 *
 * @param client Target client.
 * @param buf Message to be sent.
 * @param len Message length.
 * @param flags Send flags.
 *
 * @return Returns 0 if success (i.e: all message was sent),
 * -1 otherwise.
 *
 * @note Technically this shouldn't be necessary, since send() should
 * block until all content is sent, since _we_ don't use 'O_NONBLOCK'.
 * However, it was reported (issue #22 on GitHub) that this was
 * happening, so just to be cautious, I will keep using this routine.
 */
static ssize_t send_all(
	ws_cli_conn_t *client, const void *buf, size_t len, int flags)
{
	const char *p;
	ssize_t ret;

	/* Sanity check. */
	if (!CLIENT_VALID(client))
		return (-1);

	p = buf;
	/* clang-format off */
	pthread_mutex_lock(&client->mtx_snd);
		while (len)
		{
			ret = send(cli_fd(client), p, len, flags);
			if (ret == -1)
			{
				pthread_mutex_unlock(&client->mtx_snd);
				return (-1);
			}
			p += ret;
			len -= ret;
		}
	pthread_mutex_unlock(&client->mtx_snd);
	/* clang-format on */
	return (0);
}

/**
 * @brief Close client connection (no close handshake, this should
 * be done earlier), set appropriate state and destroy mutexes.
 *
 * @param client Client connection.
 * @param lock Should lock the global mutex?.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void close_client(ws_cli_conn_t *client, int lock)
{
	int fd;

	if (!CLIENT_VALID(client))
		return;

	set_client_state(client, WS_STATE_CLOSED);

	/*
	 * Both mutexes, always in that order: the global one keeps the legacy
	 * producers (which hold it across blocking sends) and the accept loop out
	 * of the way, the lossy one excludes ws_sendframe_lossy(). No producer can
	 * therefore be looking at - or dup()ing - this fd while it is closed, and
	 * the slot cannot be handed to a new client before its locks are gone.
	 * The join inside ws_outbox_teardown() is bounded by the sender's current
	 * select() slice, as it already was for the legacy path.
	 */
	/* clang-format off */
	if (lock)
		pthread_mutex_lock(&mutex);

			pthread_mutex_lock(&lossy_mutex);
				fd = cli_fd(client);
				cli_fd_set(client, -1);

				if (fd > -1)
					close_socket(fd);

				ws_outbox_teardown(client);
			pthread_mutex_unlock(&lossy_mutex);

			pthread_cond_destroy(&client->cnd_state_close);
			pthread_mutex_destroy(&client->mtx_state);
			pthread_mutex_destroy(&client->mtx_snd);
			pthread_mutex_destroy(&client->mtx_ping);
	if (lock)
		pthread_mutex_unlock(&mutex);
	/* clang-format on */
}

/**
 * @brief Close time-out thread.
 *
 * For a given client, this routine sleeps until
 * TIMEOUT_MS and closes the connection or returns
 * sooner if already closed connection.
 *
 * @param p ws_connection/ws_cli_conn_t Structure Pointer.
 *
 * @return Always NULL.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void *close_timeout(void *p)
{
	ws_cli_conn_t *conn = p;
	struct timespec ts;
	int state;

	pthread_mutex_lock(&conn->mtx_state);

	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_nsec += MS_TO_NS(TIMEOUT_MS);

	/* Normalize the time. */
	while (ts.tv_nsec >= 1000000000)
	{
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	while (conn->state != WS_STATE_CLOSED &&
		   pthread_cond_timedwait(&conn->cnd_state_close, &conn->mtx_state, &ts) !=
			   ETIMEDOUT)
		;

	state = conn->state;
	pthread_mutex_unlock(&conn->mtx_state);

	/* If already closed. */
	if (state == WS_STATE_CLOSED)
		goto quit;

	DEBUG("Timer expired, closing client %d\n", cli_fd(conn));

	close_client(conn, 1);
quit:
	return (NULL);
}

/**
 * @brief For a valid client index @p client, starts
 * the timeout thread and set the current state
 * to 'CLOSING'.
 *
 * @param client Client connection.
 *
 * @return Returns 0 if success, -1 otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int start_close_timeout(ws_cli_conn_t *client)
{
	if (!CLIENT_VALID(client))
		return (-1);

	pthread_mutex_lock(&client->mtx_state);

	if (client->state != WS_STATE_OPEN)
		goto out;

	client->state = WS_STATE_CLOSING;

	if (pthread_create(&client->thrd_tout, NULL, close_timeout, client))
	{
		pthread_mutex_unlock(&client->mtx_state);
		panic("Unable to create timeout thread\n");
	}
	client->close_thrd = true;
out:
	pthread_mutex_unlock(&client->mtx_state);
	return (0);
}

/**
 * @brief Initialise the outbox of @p client on first use, so that clients that
 * never use the lossy path pay nothing for it. Frame buffers of an earlier
 * client of the same slot are kept when small enough, so streaming does not
 * allocate on every frame.
 *
 * The client array is memset(-1) at startup and the magic values below are
 * only ever written here and in ws_outbox_teardown(), so neither uninitialised
 * nor retired memory can be mistaken for a usable outbox.
 *
 * Must be called with the lossy mutex held, which also excludes
 * ws_outbox_teardown() for the very same slot.
 *
 * @return 0 when the outbox is usable, -1 otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int ws_outbox_prepare(ws_cli_conn_t *client)
{
	struct ws_outbox *ob = &client->outbox;
	unsigned i;

	if (ob->magic == WS_OUTBOX_MAGIC)
		return (0);

	/* Anything but 'idle' means the memory was never initialised. */
	if (ob->magic != WS_OUTBOX_IDLE)
	{
		for (i = 0; i < WS_LOSSY_QUEUE_DEPTH; i++)
		{
			ob->slots[i].buf = NULL;
			ob->slots[i].alloc = 0;
		}
	}

	if (pthread_mutex_init(&ob->mtx, NULL))
		return (-1);
	if (pthread_cond_init(&ob->cnd, NULL))
	{
		pthread_mutex_destroy(&ob->mtx);
		return (-1);
	}

	ob->magic = WS_OUTBOX_MAGIC;
	ob->started = false;
	ob->quit = false;
	ob->dead = false;
	ob->pending = false;
	ob->wslot = 0;
	ob->fd = -1;

	for (i = 0; i < WS_LOSSY_QUEUE_DEPTH; i++)
	{
		ob->slots[i].len = 0;
		ob->slots[i].sent = 0;
	}

	return (0);
}

/**
 * @brief Push one frame out of the outbox without ever spending more than
 * WS_LOSSY_STALL_MS on it, however slowly the client drains.
 *
 * The budget is an absolute deadline, not a per-attempt timeout: a client that
 * accepts a few bytes at a time is as useless for a live stream as one that
 * accepts nothing, and it must not keep a frame open forever.
 *
 * @param ob    Outbox; ob->fd is read, the mutex is not held.
 * @param frame Slot owned by the calling sender thread.
 *
 * @return 0 when the whole frame left, -1 if the socket is unusable or the
 * client did not take the frame in time.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int ws_outbox_write(struct ws_outbox *ob, struct ws_outframe *frame)
{
	const unsigned char *p;
	struct timespec deadline;
	size_t left;
	ssize_t n;

	p = frame->buf + frame->sent;
	left = frame->len - frame->sent;

	clock_gettime(CLOCK_MONOTONIC, &deadline);
	deadline.tv_sec += WS_LOSSY_STALL_MS / 1000;
	deadline.tv_nsec += (long)(WS_LOSSY_STALL_MS % 1000) * 1000000L;
	if (deadline.tv_nsec >= 1000000000L)
	{
		deadline.tv_sec++;
		deadline.tv_nsec -= 1000000000L;
	}

	while (left)
	{
		n = send(ob->fd, p, left, MSG_DONTWAIT | MSG_NOSIGNAL);

		if (n > 0)
		{
			frame->sent += (size_t)n;
			p += (size_t)n;
			left -= (size_t)n;
			continue;
		}

		if (n < 0 && errno == EINTR)
			continue;

		if (n < 0 && errno == EAGAIN)
		{
			struct timespec now;
			fd_set wfds;
			struct timeval tv;
			long ms;
			int ret;

			clock_gettime(CLOCK_MONOTONIC, &now);
			ms = (long)(deadline.tv_sec - now.tv_sec) * 1000 +
				(deadline.tv_nsec - now.tv_nsec) / 1000000L;
			if (ms <= 0)
				return (-1);
			if (ms > WS_LOSSY_STALL_SLICE_MS)
				ms = WS_LOSSY_STALL_SLICE_MS;

			FD_ZERO(&wfds);
			FD_SET(ob->fd, &wfds);

			tv.tv_sec = ms / 1000;
			tv.tv_usec = (ms % 1000) * 1000;

			ret = select(ob->fd + 1, NULL, &wfds, NULL, &tv);
			if (ret < 0 && errno != EINTR)
				return (-1);

			continue;
		}

		return (-1);
	}

	frame->sent = 0;
	return (0);
}

/**
 * @brief Sender thread of a client outbox: writes the newest queued frame to
 * the socket until the connection dies or the client thread goes away.
 *
 * It always writes the slot the producers just stopped using, so the
 * (potentially slow) write runs without holding the outbox lock.
 *
 * @param p ws_connection/ws_cli_conn_t structure pointer.
 *
 * @return Always NULL.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void *ws_outbox_sender(void *p)
{
	ws_cli_conn_t *client = p;
	struct ws_outbox *ob = &client->outbox;
	struct ws_outframe *frame;
	int fd;
	int ret;

	for (;;)
	{
		pthread_mutex_lock(&ob->mtx);

		while (!ob->quit && !ob->pending)
			pthread_cond_wait(&ob->cnd, &ob->mtx);

		if (ob->quit)
		{
			fd = ob->fd;
			ob->fd = -1;
			pthread_mutex_unlock(&ob->mtx);

			if (fd >= 0)
				close_socket(fd);
			break;
		}

		/* Take the pending frame, producers move to the other slot. */
		frame = &ob->slots[ob->wslot];
		ob->pending = false;
		ob->wslot ^= 1u;

		pthread_mutex_unlock(&ob->mtx);

		/*
		 * Legacy callers (ws_ping, ws_close_client, ws_sendframe) still write
		 * this socket directly through send_all(), so both writers have to
		 * hold the per-client send lock or control frames would interleave
		 * with a video frame. Only the sender thread waits here, never a
		 * producer.
		 */
		pthread_mutex_lock(&client->mtx_snd);
		ret = ws_outbox_write(ob, frame);
		pthread_mutex_unlock(&client->mtx_snd);

		if (ret == 0)
			continue;

		/*
		 * Unusable socket, or a client that stopped reading: a partially
		 * written frame must never stay on the wire, so the connection is
		 * dropped. Only our own socket reference is touched here - the
		 * client thread owns the connection and tears it down as soon as
		 * its blocked recv() sees the shutdown.
		 */
		DEBUG("Lossy send failed, dropping client %d\n", cli_fd(client));

		pthread_mutex_lock(&ob->mtx);
		ob->dead = true;
		ob->pending = false;
		if (ob->fd >= 0)
		{
			shutdown(ob->fd, SHUT_RDWR);
			close(ob->fd);
			ob->fd = -1;
		}
		pthread_mutex_unlock(&ob->mtx);

		break;
	}

	return (NULL);
}

/**
 * @brief Stop and reap the sender thread of @p client, then drop the outbox
 * locks. Frame buffers small enough are kept for the next client of the slot.
 *
 * Must be called with the lossy mutex held and only once the connection is
 * gone; close_client() is the only caller. Holding that mutex across the join
 * is what makes this safe: producers are excluded so no frame buffer can be
 * touched while it is freed, and the accept loop cannot claim the slot in the
 * middle. The sender only ever takes its own outbox and send locks, never the
 * mutex we hold, so the join cannot deadlock; it costs at most a select()
 * slice plus, while a legacy sender owns the send lock, that send()'s timeout.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void ws_outbox_teardown(ws_cli_conn_t *client)
{
	struct ws_outbox *ob = &client->outbox;
	unsigned i;
	bool joined;

	if (ob->magic != WS_OUTBOX_MAGIC)
		return;

	pthread_mutex_lock(&ob->mtx);
	joined = ob->started;
	ob->quit = true;
	ob->dead = true;

	/*
	 * shutdown() wakes both a select()-waiting sender and the client thread's
	 * recv(). The fd itself is closed by the sender, never here, so it cannot
	 * be closed under a running send().
	 */
	if (ob->fd >= 0)
		shutdown(ob->fd, SHUT_RDWR);

	pthread_cond_broadcast(&ob->cnd);
	pthread_mutex_unlock(&ob->mtx);

	if (joined)
		pthread_join(ob->thread, NULL);

	for (i = 0; i < WS_LOSSY_QUEUE_DEPTH; i++)
	{
		if (ob->slots[i].buf && ob->slots[i].alloc > WS_LOSSY_KEEP_ALLOC)
		{
			free(ob->slots[i].buf);
			ob->slots[i].buf = NULL;
			ob->slots[i].alloc = 0;
		}
		ob->slots[i].len = 0;
		ob->slots[i].sent = 0;
	}

	ob->magic = WS_OUTBOX_IDLE;
	ob->started = false;
	ob->quit = false;
	ob->dead = true;
	ob->pending = false;
	ob->wslot = 0;
	ob->fd = -1;

	pthread_cond_destroy(&ob->cnd);
	pthread_mutex_destroy(&ob->mtx);
}

/**
 * @brief Queue one lossy frame for @p client, copying it into the outbox.
 *
 * Never waits for the network: if the outbox still holds a frame the sender
 * did not start, that frame is replaced by this one and @p behind is set.
 *
 * @return 1 when queued, 0 when the client cannot take it (gone), -1 on error.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int ws_outbox_enqueue(ws_cli_conn_t *client, const char *msg, size_t size,
	int type, int *behind)
{
	struct ws_outbox *ob = &client->outbox;
	struct ws_outframe *slot;
	unsigned char *nbuf;
	unsigned char hdr[10];
	size_t hdr_len;
	size_t total;
	uint64_t length;
	int replaced;

	length = (uint64_t)size;

	hdr[0] = (unsigned char)(WS_FIN | type);

	if (length <= 125)
	{
		hdr[1] = (unsigned char)(length & 0x7F);
		hdr_len = 2;
	}
	else if (length <= 65535)
	{
		hdr[1] = 126;
		hdr[2] = (unsigned char)((length >> 8) & 255);
		hdr[3] = (unsigned char)(length & 255);
		hdr_len = 4;
	}
	else
	{
		hdr[1] = 127;
		hdr[2] = (unsigned char)((length >> 56) & 255);
		hdr[3] = (unsigned char)((length >> 48) & 255);
		hdr[4] = (unsigned char)((length >> 40) & 255);
		hdr[5] = (unsigned char)((length >> 32) & 255);
		hdr[6] = (unsigned char)((length >> 24) & 255);
		hdr[7] = (unsigned char)((length >> 16) & 255);
		hdr[8] = (unsigned char)((length >> 8) & 255);
		hdr[9] = (unsigned char)(length & 255);
		hdr_len = 10;
	}

	total = hdr_len + size;

	pthread_mutex_lock(&ob->mtx);

	if (ob->dead || ob->quit || cli_fd(client) < 0)
	{
		pthread_mutex_unlock(&ob->mtx);
		return (0);
	}

	slot = &ob->slots[ob->wslot];

	if (slot->alloc < total)
	{
		nbuf = realloc(slot->buf, total);
		if (!nbuf)
		{
			pthread_mutex_unlock(&ob->mtx);
			return (-1);
		}
		slot->buf = nbuf;
		slot->alloc = total;
	}

	replaced = ob->pending ? 1 : 0;

	memcpy(slot->buf, hdr, hdr_len);
	memcpy(slot->buf + hdr_len, msg, size);
	slot->len = total;
	slot->sent = 0;
	ob->pending = true;

	if (!ob->started)
	{
		/*
		 * Own reference to the socket: the client thread may close the
		 * connection at any time and this dup keeps the sender's fd valid
		 * until the sender is done with it.
		 */
		ob->fd = dup(cli_fd(client));
		if (ob->fd < 0 || pthread_create(&ob->thread, NULL, ws_outbox_sender, client))
		{
			if (ob->fd >= 0)
			{
				close_socket(ob->fd);
				ob->fd = -1;
			}
			ob->dead = true;
			ob->pending = false;
			pthread_mutex_unlock(&ob->mtx);
			return (-1);
		}
		ob->started = true;
	}

	pthread_cond_signal(&ob->cnd);
	pthread_mutex_unlock(&ob->mtx);

	if (behind && replaced)
		*behind = 1;

	return (1);
}

/**
 * @brief Sets the IP address relative to a client connection opened
 * by the server and save inside the client structure.
 *
 * @param client Client connection.
 */
static void set_client_address(ws_cli_conn_t *client)
{
	struct sockaddr_in addr;
	socklen_t addr_size;

	if (!CLIENT_VALID(client))
		return;

	addr_size = sizeof(struct sockaddr_in);

	if (getpeername(cli_fd(client), (struct sockaddr *)&addr, &addr_size) < 0)
		return;

	memset(client->ip, 0, sizeof(client->ip));
	inet_ntop(AF_INET, &addr.sin_addr, client->ip, INET_ADDRSTRLEN);
}

/**
 * @brief Gets the IP address relative to a client connection opened
 * by the server.
 *
 * @param client Client connection.
 *
 * @return Pointer the ip address, or NULL if fails.
 *
 * @note The returned string is static, no need to free up memory.
 */
char *ws_getaddress(ws_cli_conn_t *client)
{
	if (!CLIENT_VALID(client))
		return (NULL);

	return (client->ip);
}

/**
 * @brief Creates and send an WebSocket frame with some payload data.
 *
 * This routine is intended to be used to create a websocket frame for
 * a given type e sending to the client. For higher level routines,
 * please check @ref ws_sendframe_txt and @ref ws_sendframe_bin.
 *
 * @param client Target to be send. If NULL, broadcast the message.
 * @param msg    Message to be send.
 * @param size   Binary message size.
 * @param type   Frame type.
 *
 * @return Returns the number of bytes written, -1 if error.
 *
 * @note If @p size is -1, it is assumed that a text frame is being sent,
 * otherwise, a binary frame. In the later case, the @p size is used.
 */
int ws_sendframe(ws_cli_conn_t *client, const char *msg, uint64_t size, int type)
{
	unsigned char *response; /* Response data.     */
	unsigned char frame[10]; /* Frame.             */
	uint8_t idx_first_rData; /* Index data.        */
	uint64_t length;         /* Message length.    */
	int idx_response;        /* Index response.    */
	ssize_t output;          /* Bytes sent.        */
	ssize_t send_ret;        /* Ret send function  */
	uint64_t i;              /* Loop index.        */
	ws_cli_conn_t *cli;      /* Client.            */

	frame[0] = (WS_FIN | type);
	length = (uint64_t)size;

	/* Split the size between octets. */
	if (length <= 125)
	{
		frame[1] = length & 0x7F;
		idx_first_rData = 2;
	}

	/* Size between 126 and 65535 bytes. */
	else if (length >= 126 && length <= 65535)
	{
		frame[1] = 126;
		frame[2] = (length >> 8) & 255;
		frame[3] = length & 255;
		idx_first_rData = 4;
	}

	/* More than 65535 bytes. */
	else
	{
		frame[1] = 127;
		frame[2] = (unsigned char)((length >> 56) & 255);
		frame[3] = (unsigned char)((length >> 48) & 255);
		frame[4] = (unsigned char)((length >> 40) & 255);
		frame[5] = (unsigned char)((length >> 32) & 255);
		frame[6] = (unsigned char)((length >> 24) & 255);
		frame[7] = (unsigned char)((length >> 16) & 255);
		frame[8] = (unsigned char)((length >> 8) & 255);
		frame[9] = (unsigned char)(length & 255);
		idx_first_rData = 10;
	}

	/* Add frame bytes. */
	idx_response = 0;
	response = malloc(sizeof(unsigned char) * (idx_first_rData + length + 1));
	if (!response)
		return (-1);

	for (i = 0; i < idx_first_rData; i++)
	{
		response[i] = frame[i];
		idx_response++;
	}

	/* Add data bytes. */
	for (i = 0; i < length; i++)
	{
		response[idx_response] = msg[i];
		idx_response++;
	}

	response[idx_response] = '\0';

	/* Send to the client if there is one. */
	output = 0;
	if (client)
		output = SEND(client, response, idx_response);

	/* If no client specified, broadcast to everyone. */
	if (!client)
	{
		pthread_mutex_lock(&mutex);

		for (i = 0; i < MAX_CLIENTS; i++)
		{
			cli = &client_socks[i];
			if ((cli_fd(cli) > -1) && get_client_state(cli) == WS_STATE_OPEN)
			{
				if ((send_ret = SEND(cli, response, idx_response)) != -1)
					output += send_ret;
				else
				{
					output = -1;
					break;
				}
			}
		}
		pthread_mutex_unlock(&mutex);
	}

	free(response);
	return ((int)output);
}

/**
 * @brief Given a PONG message, decodes the content
 * as a int32_t number that corresponds to our
 * PONG id.
 *
 * @param msg Content to be decoded.
 *
 * @return Returns the PONG id.
 */
static inline int32_t pong_msg_to_int32(uint8_t *msg)
{
	int32_t pong_id;
	/* Decodes as big-endian. */
	pong_id = (msg[3] << 0) | (msg[2] << 8) | (msg[1] << 16) | (msg[0] << 24);
	return (pong_id);
}

/**
 * @brief Given a PING id, encodes the content to be sent
 * as payload of a PING frame.
 *
 * @param ping_id PING id to be encoded.
 * @param msg Target buffer.
 */
static inline void int32_to_ping_msg(int32_t ping_id, uint8_t *msg)
{
	/* Encodes as big-endian. */
	msg[0] = (ping_id >> 24);
	msg[1] = (ping_id >> 16);
	msg[2] = (ping_id >>  8);
	msg[3] = (ping_id >>  0);
}

/**
 * @brief Send a ping message and close if the client surpasses
 * the threshold imposed.
 *
 * @param cli Client to be sent.
 * @param threshold How many pings can miss?.
 * @param lock Should lock global mutex or not?.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void send_ping_close(ws_cli_conn_t *cli, int threshold, int lock)
{
	uint8_t ping_msg[4];

	if (!CLIENT_VALID(cli) || get_client_state(cli) != WS_STATE_OPEN)
		return;

	/* clang-format off */
	pthread_mutex_lock(&cli->mtx_ping);

		cli->current_ping_id++;
		int32_to_ping_msg(cli->current_ping_id, ping_msg);

		/* Send PING. */
		ws_sendframe(cli, (const char*)ping_msg, sizeof(ping_msg), WS_FR_OP_PING);

		/* Check previous PONG: if greater than threshold, abort. */
		if ((cli->current_ping_id - cli->last_pong_id) > threshold)
			close_client(cli, lock);

	pthread_mutex_unlock(&cli->mtx_ping);
	/* clang-format on */
}

/**
 * @brief Sends a PING frame to the client @p cli with threshold
 * @p threshold.
 *
 * This routine sends a PING to a single client pointed to by
 * @p cli or a broadcast PING if @p cli is NULL. If the specified
 * client does not respond up to @p threshold PINGs, the connection
 * is aborted.
 *
 * ws_ping() is not automatic: the user who wants to send keep-alive
 * PINGs *must* call this routine in a timely manner, whether on
 * a different thread or inside an event.
 *
 * See examples/ping/ping.c for a minimal example usage.
 *
 * @param cli       Client to be sent, if NULL, broadcast.
 * @param threshold How many ignored PINGs should tolerate? (should be
 *                  positive and greater than 0).
 *
 * @note It should be noted that the time between each call to
 * ws_ping() is the timeout itself for receiving the PONG.
 *
 * It is also important to note that for devices with unstable
 * connections (such as a weak WiFi signal or 3/4/5G from a cell phone),
 * a threshold greater than 1 is advisable.
 */
void ws_ping(ws_cli_conn_t *cli, int threshold)
{
	int i;

	/* Sanity check. */
	if (threshold <= 0)
		return;

	/* PING a single client. */
	if (cli)
		send_ping_close(cli, threshold, 1);

	/* PING broadcast. */
	else
	{
		/* clang-format off */
		pthread_mutex_lock(&mutex);
			for (i = 0; i < MAX_CLIENTS; i++)
				send_ping_close(&client_socks[i], threshold, 0);
		pthread_mutex_unlock(&mutex);
		/* clang-format on */
	}
}

/**
 * @brief Sends a WebSocket text frame.
 *
 * @param client Target to be send. If NULL, broadcast the message.
 * @param msg    Message to be send, null terminated.
 *
 * @return Returns the number of bytes written, -1 if error.
 */
int ws_sendframe_txt(ws_cli_conn_t *client, const char *msg)
{
	return ws_sendframe(client, msg, (uint64_t)strlen(msg), WS_FR_OP_TXT);
}

/**
 * @brief Sends a WebSocket binary frame.
 *
 * @param client Target to be send. If NULL, broadcast the message.
 * @param msg    Message to be send.
 * @param size   Binary message size.
 *
 * @return Returns the number of bytes written, -1 if error.
 */
int ws_sendframe_bin(ws_cli_conn_t *client, const char *msg, uint64_t size)
{
	return ws_sendframe(client, msg, size, WS_FR_OP_BIN);
}

/**
 * @brief Creates and queues a WebSocket frame without ever waiting for a
 * client. See ws_sendframe_lossy() in the header for the semantics.
 */
int ws_sendframe_lossy(ws_cli_conn_t *client, const char *msg, uint64_t size,
	int type, int *behind)
{
	ws_cli_conn_t *cli;
	unsigned i;
	int queued;
	int ret;

	if (behind)
		*behind = 0;

	if (!msg || size > WS_LOSSY_MAX_FRAME)
		return (-1);

	/*
	 * The lossy mutex is held for the whole enqueue. It serialises us against
	 * the accept loop filling slots and against close_client() retiring a
	 * slot - including its sender thread - and, unlike the global mutex, it
	 * is never held across a blocking send(), so a stalled legacy sender
	 * cannot delay this caller. Enqueuing only copies and signals.
	 */
	pthread_mutex_lock(&lossy_mutex);

	/* Single client. */
	if (client)
	{
		if (!CLIENT_VALID(client) || get_client_state(client) != WS_STATE_OPEN)
		{
			pthread_mutex_unlock(&lossy_mutex);
			return (-1);
		}

		if (ws_outbox_prepare(client) < 0)
		{
			pthread_mutex_unlock(&lossy_mutex);
			return (-1);
		}

		ret = ws_outbox_enqueue(client, msg, (size_t)size, type, behind);

		pthread_mutex_unlock(&lossy_mutex);

		return (ret < 0 ? -1 : (ret ? 1 : 0));
	}

	/* Broadcast: enqueue per client, the caller never touches the sockets. */
	queued = 0;

	for (i = 0; i < MAX_CLIENTS; i++)
	{
		cli = &client_socks[i];

		if (cli_fd(cli) < 0 || get_client_state(cli) != WS_STATE_OPEN)
			continue;

		if (ws_outbox_prepare(cli) < 0)
			continue;

		if (ws_outbox_enqueue(cli, msg, (size_t)size, type, behind) == 1)
			queued++;
	}

	pthread_mutex_unlock(&lossy_mutex);

	return (queued);
}

/**
 * @brief Queues a WebSocket binary frame without ever waiting for a client.
 */
int ws_sendframe_bin_lossy(ws_cli_conn_t *client, const char *msg, uint64_t size,
	int *behind)
{
	return ws_sendframe_lossy(client, msg, size, WS_FR_OP_BIN, behind);
}

/**
 * @brief For a given @p client, gets the current state for
 * the connection, or -1 if invalid.
 *
 * @param client Client connection.
 *
 * @return Returns the connection state or -1 if
 * invalid @p client.
 *
 * @see WS_STATE_CONNECTING
 * @see WS_STATE_OPEN
 * @see WS_STATE_CLOSING
 * @see WS_STATE_CLOSED
 */
int ws_get_state(ws_cli_conn_t *client)
{
	return (get_client_state(client));
}

/**
 * @brief Close the client connection for the given @p
 * client with normal close code (1000) and no reason
 * string.
 *
 * @param client Client connection.
 *
 * @return Returns 0 on success, -1 otherwise.
 *
 * @note If the client did not send a close frame in
 * TIMEOUT_MS milliseconds, the server will close the
 * connection with error code (1002).
 */
int ws_close_client(ws_cli_conn_t *client)
{
	unsigned char clse_code[2];
	int cc;

	/* Check if client is a valid and connected client. */
	if (!CLIENT_VALID(client) || cli_fd(client) == -1)
		return (-1);

	/*
	 * Instead of using do_close(), we use this to avoid using
	 * msg_ctrl buffer from wfd and avoid a race condition
	 * if this is invoked asynchronously.
	 */
	cc = WS_CLSE_NORMAL;
	clse_code[0] = (cc >> 8);
	clse_code[1] = (cc & 0xFF);
	if (ws_sendframe(
			client, (const char *)clse_code, sizeof(char) * 2, WS_FR_OP_CLSE) < 0)
	{
		DEBUG("An error has occurred while sending closing frame!\n");
		return (-1);
	}

	/*
	 * Starts the timeout thread: if the client did not send
	 * a close frame in TIMEOUT_MS milliseconds, the server
	 * will close the connection with error code (1002).
	 */
	start_close_timeout(client);
	return (0);
}

/**
 * @brief Checks is a given opcode @p frame
 * belongs to a control frame or not.
 *
 * @param frame Frame opcode to be checked.
 *
 * @return Returns 1 if is a control frame, 0 otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static inline int is_control_frame(int frame)
{
	return (
		frame == WS_FR_OP_CLSE || frame == WS_FR_OP_PING || frame == WS_FR_OP_PONG);
}

/**
 * @brief Do the handshake process.
 *
 * @param wfd Websocket Frame Data.
 *
 * @return Returns 0 if success, a negative number otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int do_handshake(struct ws_frame_data *wfd)
{
	char *response; /* Handshake response message. */
	char *p;        /* Last request line pointer.  */
	ssize_t n;      /* Read/Write bytes.           */

	/* Read the very first client message. */
	if ((n = RECV(wfd->client, wfd->frm, sizeof(wfd->frm) - 1)) < 0)
		return (-1);

	/* Advance our pointers before the first next_byte(). */
	p = strstr((const char *)wfd->frm, "\r\n\r\n");
	if (p == NULL)
	{
		DEBUG("An empty line with \\r\\n was expected!\n");
		return (-1);
	}
	wfd->amt_read = n;
	wfd->cur_pos = (size_t)((ptrdiff_t)(p - (char *)wfd->frm)) + 4;

	/* Get response. */
	if (get_handshake_response((char *)wfd->frm, &response) < 0)
	{
		DEBUG("Cannot get handshake response, request was: %s\n", wfd->frm);
		return (-1);
	}

	/* Valid request. */
	DEBUG("Handshaked, response: \n"
		  "------------------------------------\n"
		  "%s"
		  "------------------------------------\n",
		response);

	/* Send handshake. */
	if (SEND(wfd->client, response, strlen(response)) < 0)
	{
		free(response);
		DEBUG("As error has occurred while handshaking!\n");
		return (-1);
	}

	/* Trigger events and clean up buffers. */
	cli_events.onopen(wfd->client);
	free(response);
	return (0);
}

/**
 * @brief Sends a close frame, accordingly with the @p close_code
 * or the message inside @p wfd.
 *
 * @param wfd Websocket Frame Data.
 * @param close_code Websocket close code.
 *
 * @return Returns 0 if success, a negative number otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int do_close(struct ws_frame_data *wfd, int close_code)
{
	int cc; /* Close code.           */

	/* If custom close-code. */
	if (close_code != -1)
	{
		cc = close_code;
		goto custom_close;
	}

	/* If empty or have a close reason, just re-send. */
	if (wfd->frame_size == 0 || wfd->frame_size > 2)
		goto send;

	/* Parse close code and check if valid, if not, we issue an protocol error.
	 */
	if (wfd->frame_size == 1)
		cc = wfd->msg_ctrl[0];
	else
		cc = ((int)wfd->msg_ctrl[0]) << 8 | wfd->msg_ctrl[1];

	/* Check if it's not valid, if so, we send a protocol error (1002). */
	if ((cc < 1000 || cc > 1003) && (cc < 1007 || cc > 1011) &&
		(cc < 3000 || cc > 4999))
	{
		cc = WS_CLSE_PROTERR;

	custom_close:
		wfd->msg_ctrl[0] = (cc >> 8);
		wfd->msg_ctrl[1] = (cc & 0xFF);

		if (ws_sendframe(wfd->client, (const char *)wfd->msg_ctrl, sizeof(char) * 2,
				WS_FR_OP_CLSE) < 0)
		{
			DEBUG("An error has occurred while sending closing frame!\n");
			return (-1);
		}
		return (0);
	}

	/* Send the data inside wfd->msg_ctrl. */
send:
	if (ws_sendframe(wfd->client, (const char *)wfd->msg_ctrl, wfd->frame_size,
			WS_FR_OP_CLSE) < 0)
	{
		DEBUG("An error has occurred while sending closing frame!\n");
		return (-1);
	}
	return (0);
}

/**
 * @brief Send a pong frame in response to a ping frame.
 *
 * Accordingly to the RFC, a pong frame must have the same
 * data payload as the ping frame, so we just send a
 * ordinary frame with PONG opcode.
 *
 * @param wfd Websocket frame data.
 *
 * @param frame_size Pong frame size.
 *
 * @return Returns 0 if success and a negative number
 * otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int do_pong(struct ws_frame_data *wfd, uint64_t frame_size)
{
	if (ws_sendframe(
			wfd->client, (const char *)wfd->msg_ctrl, frame_size, WS_FR_OP_PONG) < 0)
	{
		wfd->error = 1;
		DEBUG("An error has occurred while ponging!\n");
		return (-1);
	}
	return (0);
}

/**
 * @brief Read a chunk of bytes and return the next byte
 * belonging to the frame.
 *
 * @param wfd Websocket Frame Data.
 *
 * @return Returns the byte read, or -1 if error.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static inline int next_byte(struct ws_frame_data *wfd)
{
	ssize_t n;

	/* If empty or full. */
	if (wfd->cur_pos == 0 || wfd->cur_pos == wfd->amt_read)
	{
		if ((n = RECV(wfd->client, wfd->frm, sizeof(wfd->frm))) <= 0)
		{
			wfd->error = 1;
			DEBUG("An error has occurred while trying to read next byte\n");
			return (-1);
		}
		wfd->amt_read = (size_t)n;
		wfd->cur_pos = 0;
	}
	return (wfd->frm[wfd->cur_pos++]);
}

/**
 * @brief Skips @p frame_size bytes of the current frame.
 *
 * @param wfd Websocket Frame Data.
 * @param frame_size Amount of bytes to be skipped.
 *
 * @return Returns 0 if success, a negative number
 * otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int skip_frame(struct ws_frame_data *wfd, uint64_t frame_size)
{
	uint64_t i;
	for (i = 0; i < frame_size; i++)
	{
		if (next_byte(wfd) == -1)
		{
			wfd->error = 1;
			return (-1);
		}
	}
	return (0);
}

/**
 * @brief Reads the current frame isolating data from control frames.
 * The parameters are changed in order to reflect the current state.
 *
 * @param wfd Websocket Frame Data.
 * @param opcode Frame opcode.
 * @param buf Buffer to be written.
 * @param frame_length Length of the current frame.
 * @param frame_size Total size of the frame (considering CONT frames)
 *                   read until the moment.
 * @param msg_idx Message index, reflects the current buffer pointer state.
 * @param masks Masks vector.
 * @param is_fin Is FIN frame indicator.
 *
 * @return Returns 0 if success, a negative number otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int read_frame(struct ws_frame_data *wfd,
	int opcode,
	unsigned char **buf,
	uint64_t *frame_length,
	uint64_t *frame_size,
	uint64_t *msg_idx,
	uint8_t *masks,
	int is_fin)
{
	unsigned char *tmp; /* Tmp message.     */
	unsigned char *msg; /* Current message. */
	int cur_byte;       /* Curr byte read.  */
	uint64_t i;         /* Loop index.      */

	msg = *buf;

	/* Decode masks and length for 16-bit messages. */
	if (*frame_length == 126)
		*frame_length = (((uint64_t)next_byte(wfd)) << 8) | next_byte(wfd);

	/* 64-bit messages. */
	else if (*frame_length == 127)
	{
		*frame_length =
			(((uint64_t)next_byte(wfd)) << 56) | /* frame[2]. */
			(((uint64_t)next_byte(wfd)) << 48) | /* frame[3]. */
			(((uint64_t)next_byte(wfd)) << 40) | (((uint64_t)next_byte(wfd)) << 32) |
			(((uint64_t)next_byte(wfd)) << 24) | (((uint64_t)next_byte(wfd)) << 16) |
			(((uint64_t)next_byte(wfd)) << 8) |
			(((uint64_t)next_byte(wfd))); /* frame[9]. */
	}

	*frame_size += *frame_length;

	/*
	 * Check frame size
	 *
	 * We need to limit the amount supported here, since if
	 * we follow strictly to the RFC, we have to allow 2^64
	 * bytes. Also keep in mind that this is still true
	 * for continuation frames.
	 */
	if (*frame_size > MAX_FRAME_LENGTH)
	{
		DEBUG("Current frame from client %d, exceeds the maximum\n"
			  "amount of bytes allowed (%" PRId64 "/%d)!",
			cli_fd(wfd->client), *frame_size + *frame_length, MAX_FRAME_LENGTH);

		wfd->error = 1;
		return (-1);
	}

	/* Read masks. */
	masks[0] = next_byte(wfd);
	masks[1] = next_byte(wfd);
	masks[2] = next_byte(wfd);
	masks[3] = next_byte(wfd);

	/*
	 * Abort if error.
	 *
	 * This is tricky: we may have multiples error codes from the
	 * previous next_bytes() calls, but, since we're only setting
	 * variables and flags, there is no major issue in setting
	 * them wrong _if_ we do not use their values, thing that
	 * we do here.
	 */
	if (wfd->error)
		return (-1);

	/*
	 * Allocate memory.
	 *
	 * The statement below will allocate a new chunk of memory
	 * if msg is NULL with size total_length. Otherwise, it will
	 * resize the total memory accordingly with the message index
	 * and if the current frame is a FIN frame or not, if so,
	 * increment the size by 1 to accommodate the line ending \0.
	 */
	if (*frame_length > 0)
	{
		if (!is_control_frame(opcode))
		{
			tmp = realloc(
				msg, sizeof(unsigned char) * (*msg_idx + *frame_length + is_fin));
			if (!tmp)
			{
				DEBUG("Cannot allocate memory, requested: % " PRId64 "\n",
					(*msg_idx + *frame_length + is_fin));

				wfd->error = 1;
				return (-1);
			}
			msg = tmp;
			*buf = msg;
		}

		/* Copy to the proper location. */
		for (i = 0; i < *frame_length; i++, (*msg_idx)++)
		{
			/* We were able to read? .*/
			cur_byte = next_byte(wfd);
			if (cur_byte == -1)
				return (-1);

			msg[*msg_idx] = cur_byte ^ masks[i % 4];
		}
	}

	/* If we're inside a FIN frame, lets... */
	if (is_fin && *frame_size > 0)
	{
		/* Increase memory if our FIN frame is of length 0. */
		if (!*frame_length && !is_control_frame(opcode))
		{
			tmp = realloc(msg, sizeof(unsigned char) * (*msg_idx + 1));
			if (!tmp)
			{
				DEBUG("Cannot allocate memory, requested: %" PRId64 "\n",
					(*msg_idx + 1));

				wfd->error = 1;
				return (-1);
			}
			msg = tmp;
			*buf = msg;
		}
		msg[*msg_idx] = '\0';
	}

	return (0);
}

/**
 * @brief Reads the next frame, whether if a TXT/BIN/CLOSE
 * of arbitrary size.
 *
 * @param wfd Websocket Frame Data.
 *
 * @return Returns 0 if success, a negative number otherwise.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static int next_frame(struct ws_frame_data *wfd)
{
	unsigned char *msg_data; /* Data frame.                */
	unsigned char *msg_ctrl; /* Control frame.             */
	uint8_t masks_data[4];   /* Masks data frame array.    */
	uint8_t masks_ctrl[4];   /* Masks control frame array. */
	uint64_t msg_idx_data;   /* Current msg index.         */
	uint64_t msg_idx_ctrl;   /* Current msg index.         */
	uint64_t frame_length;   /* Frame length.              */
	uint64_t frame_size;     /* Current frame size.        */
	uint32_t utf8_state;     /* Current UTF-8 state.       */
	int32_t pong_id;         /* Current PONG id.           */
	uint8_t opcode;          /* Frame opcode.              */
	uint8_t is_fin;          /* Is FIN frame flag.         */
	uint8_t mask;            /* Mask.                      */
	int cur_byte;            /* Current frame byte.        */

	msg_data = NULL;
	msg_ctrl = wfd->msg_ctrl;
	is_fin = 0;
	frame_length = 0;
	frame_size = 0;
	msg_idx_data = 0;
	msg_idx_ctrl = 0;
	wfd->frame_size = 0;
	wfd->frame_type = -1;
	wfd->msg = NULL;
	utf8_state = UTF8_ACCEPT;

	/* Read until find a FIN or a unsupported frame. */
	do
	{
		/*
		 * Obs: next_byte() can return error if not possible to read the
		 * next frame byte, in this case, we return an error.
		 *
		 * However, please note that this check is only made here and in
		 * the subsequent next_bytes() calls this also may occur too.
		 * wsServer is assuming that the client only create right
		 * frames and we will do not have disconnections while reading
		 * the frame but just when waiting for a frame.
		 */
		cur_byte = next_byte(wfd);
		if (cur_byte == -1)
			return (-1);

		is_fin = (cur_byte & 0xFF) >> WS_FIN_SHIFT;
		opcode = (cur_byte & 0xF);

		/*
		 * Check for RSV field.
		 *
		 * Since wsServer do not negotiate extensions if we receive
		 * a RSV field, we must drop the connection.
		 */
		if (cur_byte & 0x70)
		{
			DEBUG("RSV is set while wsServer do not negotiate extensions!\n");
			wfd->error = 1;
			break;
		}

		/*
		 * Check if the current opcode makes sense:
		 * a) If we're inside a cont frame but no previous data frame
		 *
		 * b) If we're handling a data-frame and receive another data
		 *    frame. (it's expected to receive only CONT or control
		 *    frames).
		 *
		 * It is worth to note that in a), we do not need to check
		 * if the previous frame was FIN or not: if was FIN, an
		 * on_message event was triggered and this function returned;
		 * so the only possibility here is a previous non-FIN data
		 * frame, ;-).
		 */
		if ((wfd->frame_type == -1 && opcode == WS_FR_OP_CONT) ||
			(wfd->frame_type != -1 && !is_control_frame(opcode) &&
				opcode != WS_FR_OP_CONT))
		{
			DEBUG("Unexpected frame was received!, opcode: %d, previous: %d\n",
				opcode, wfd->frame_type);
			wfd->error = 1;
			break;
		}

		/* Check if one of the valid opcodes. */
		if (opcode == WS_FR_OP_TXT || opcode == WS_FR_OP_BIN ||
			opcode == WS_FR_OP_CONT || opcode == WS_FR_OP_PING ||
			opcode == WS_FR_OP_PONG || opcode == WS_FR_OP_CLSE)
		{
			/*
			 * Check our current state: if CLOSING, we only accept close
			 * frames.
			 *
			 * Since the server may, at any time, asynchronously, asks
			 * to close the client connection, we should terminate
			 * immediately.
			 */
			if (get_client_state(wfd->client) == WS_STATE_CLOSING &&
				opcode != WS_FR_OP_CLSE)
			{
				DEBUG("Unexpected frame received, expected CLOSE (%d), "
					  "received: (%d)",
					WS_FR_OP_CLSE, opcode);
				wfd->error = 1;
				break;
			}

			/* Only change frame type if not a CONT frame. */
			if (opcode != WS_FR_OP_CONT && !is_control_frame(opcode))
				wfd->frame_type = opcode;

			mask = next_byte(wfd);
			frame_length = mask & 0x7F;
			frame_size = 0;
			msg_idx_ctrl = 0;

			/*
			 * We should deny non-FIN control frames or that have
			 * more than 125 octets.
			 */
			if (is_control_frame(opcode) && (!is_fin || frame_length > 125))
			{
				DEBUG("Control frame bigger than 125 octets or not a FIN "
					  "frame!\n");
				wfd->error = 1;
				break;
			}

			/* Normal data frames. */
			if (opcode == WS_FR_OP_TXT || opcode == WS_FR_OP_BIN ||
				opcode == WS_FR_OP_CONT)
			{
				if (read_frame(wfd, opcode, &msg_data, &frame_length,
						&wfd->frame_size, &msg_idx_data, masks_data, is_fin) < 0)
					break;

#ifdef VALIDATE_UTF8
				/* UTF-8 Validate partial (or not) frame. */
				if (wfd->frame_type == WS_FR_OP_TXT)
				{
					if (is_fin)
					{
						if (is_utf8_len_state(
								msg_data + (msg_idx_data - frame_length),
								frame_length, utf8_state) != UTF8_ACCEPT)
						{
							DEBUG("Dropping invalid complete message!\n");
							wfd->error = 1;
							do_close(wfd, WS_CLSE_INVUTF8);
						}
					}

					/* Check current state for a CONT or initial TXT frame. */
					else
					{
						utf8_state = is_utf8_len_state(
							msg_data + (msg_idx_data - frame_length), frame_length,
							utf8_state);

						/* We can be in any state, except reject. */
						if (utf8_state == UTF8_REJECT)
						{
							DEBUG("Dropping invalid cont/initial frame!\n");
							wfd->error = 1;
							do_close(wfd, WS_CLSE_INVUTF8);
						}
					}
				}
#endif
			}

			/*
			 * We _may_ send a PING frame if the ws_ping() routine was invoked.
			 *
			 * If the content is invalid and/or differs the size, ignore it.
			 * (maybe unsolicited PONG).
			 */
			else if (opcode == WS_FR_OP_PONG)
			{
				if (read_frame(wfd, opcode, &msg_ctrl, &frame_length, &frame_size,
						&msg_idx_ctrl, masks_ctrl, is_fin) < 0)
					break;

				is_fin = 0;

				/* If there is no content and/or differs the size, ignore it. */
				if (frame_size != sizeof(wfd->client->last_pong_id))
					continue;

				/*
				 * Our PONG id should be positive and smaller than our
				 * current PING id. If not, ignore.
				 */
				/* clang-format off */
				pthread_mutex_lock(&wfd->client->mtx_ping);

					pong_id = pong_msg_to_int32(msg_ctrl);
					if (pong_id < 0 || pong_id > wfd->client->current_ping_id)
					{
						pthread_mutex_unlock(&wfd->client->mtx_ping);
						continue;
					}
					wfd->client->last_pong_id = pong_id;

				pthread_mutex_unlock(&wfd->client->mtx_ping);
				/* clang-format on */
				continue;
			}

			/* We should answer to a PING frame as soon as possible. */
			else if (opcode == WS_FR_OP_PING)
			{
				if (read_frame(wfd, opcode, &msg_ctrl, &frame_length, &frame_size,
						&msg_idx_ctrl, masks_ctrl, is_fin) < 0)
					break;

				if (do_pong(wfd, frame_size) < 0)
					break;

				/* Quick hack to keep our loop. */
				is_fin = 0;
			}

			/* We interrupt the loop as soon as we find a CLOSE frame. */
			else
			{
				if (read_frame(wfd, opcode, &msg_ctrl, &frame_length, &frame_size,
						&msg_idx_ctrl, masks_ctrl, is_fin) < 0)
					break;

#ifdef VALIDATE_UTF8
				/* If there is a close reason, check if it is UTF-8 valid. */
				if (frame_size > 2 && !is_utf8_len(msg_ctrl + 2, frame_size - 2))
				{
					DEBUG("Invalid close frame payload reason! (not UTF-8)\n");
					wfd->error = 1;
					break;
				}
#endif

				/* Since we're aborting, we can scratch the 'data'-related
				 * vars here. */
				wfd->frame_size = frame_size;
				wfd->frame_type = WS_FR_OP_CLSE;
				free(msg_data);
				return (0);
			}
		}

		/* Anything else (unsupported frames). */
		else
		{
			DEBUG("Unsupported frame opcode: %d\n", opcode);
			/* We should consider as error receive an unknown frame. */
			wfd->frame_type = opcode;
			wfd->error = 1;
		}

	} while (!is_fin && !wfd->error);

	/* Check for error. */
	if (wfd->error)
	{
		free(msg_data);
		wfd->msg = NULL;
		return (-1);
	}

	wfd->msg = msg_data;
	return (0);
}

/**
 * @brief Establishes to connection with the client and trigger
 * events when occurs one.
 *
 * @param vclient Client connection.
 *
 * @return Returns @p vclient.
 *
 * @note This will be run on a different thread.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void *ws_establishconnection(void *vclient)
{
	struct ws_frame_data wfd; /* WebSocket frame data.   */
	ws_cli_conn_t *client;    /* Client structure.       */
	int clse_thrd;            /* Time-out close thread.  */

	client = vclient;

	/* Prepare frame data. */
	memset(&wfd, 0, sizeof(wfd));
	wfd.client = client;

	/* Do handshake. */
	if (do_handshake(&wfd) < 0)
		goto closed;

	/* Change state. */
	set_client_state(client, WS_STATE_OPEN);

	/* Read next frame until client disconnects or an error occur. */
	while (next_frame(&wfd) >= 0)
	{
		/* Text/binary event. */
		if ((wfd.frame_type == WS_FR_OP_TXT || wfd.frame_type == WS_FR_OP_BIN) &&
			!wfd.error)
		{
			cli_events.onmessage(client, wfd.msg, wfd.frame_size, wfd.frame_type);
		}

		/* Close event. */
		else if (wfd.frame_type == WS_FR_OP_CLSE && !wfd.error)
		{
			/*
			 * We only send a CLOSE frame once, if we're already
			 * in CLOSING state, there is no need to send.
			 */
			if (get_client_state(client) != WS_STATE_CLOSING)
			{
				set_client_state(client, WS_STATE_CLOSING);

				/* We only send a close frameSend close frame */
				do_close(&wfd, -1);
			}

			free(wfd.msg);
			break;
		}

		free(wfd.msg);
	}

	/*
	 * on_close events always occur, whether for client closure
	 * or server closure, as the server is expected to
	 * always know when the client disconnects.
	 */
	cli_events.onclose(client);

closed:
	clse_thrd = client->close_thrd;

	/* Wait for timeout thread if necessary. */
	if (clse_thrd)
	{
		pthread_cond_signal(&client->cnd_state_close);
		pthread_join(client->thrd_tout, NULL);
	}

	/* Close connectin properly. */
	if (get_client_state(client) != WS_STATE_CLOSED)
		close_client(client, 1);

	return (vclient);
}

/**
 * @brief Main loop that keeps accepting new connections.
 *
 * @param data Server socket.
 *
 * @return Returns @p data.
 *
 * @note This may be run on a different thread.
 *
 * @attention This is part of the internal API and is documented just
 * for completeness.
 */
static void *ws_accept(void *data)
{
	struct sockaddr_in client; /* Client.                */
	pthread_t client_thread;   /* Client thread.         */
	struct timeval time;       /* Client socket timeout. */
	int new_sock;              /* New opened connection. */
	int sock;                  /* Server sock.           */
	int len;                   /* Length of sockaddr.    */
	int i;                     /* Loop index.            */
	int on = 1;

	sock = *(int *)data;
	len = sizeof(struct sockaddr_in);

	while (1)
	{
		/* Accept. */
		new_sock = accept(sock, (struct sockaddr *)&client, (socklen_t *)&len);

		if (new_sock < 0)
			panic("Error on accepting connections..");

		/*
		 * Streaming frames must leave immediately instead of waiting for
		 * more data to coalesce, and a short unsent queue is what makes a
		 * stalled client show up as EAGAIN early instead of after
		 * megabytes of kernel buffering.
		 */
		setsockopt(new_sock, IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(on));

#ifdef TCP_NOTSENT_LOWAT
		{
			int lowat = WS_LOSSY_NOTSENT_LOWAT;
			setsockopt(new_sock, SOL_SOCKET, TCP_NOTSENT_LOWAT, &lowat,
				sizeof(lowat));
		}
#endif

		if (timeout)
		{
			time.tv_sec = timeout / 1000;
			time.tv_usec = (timeout % 1000) * 1000;

			/*
			 * Socket timeout
			 * This feature seems to be supported on Linux, Windows,
			 * macOS and FreeBSD.
			 *
			 * See:
			 *   https://linux.die.net/man/3/setsockopt
			 */
			setsockopt(new_sock, SOL_SOCKET, SO_SNDTIMEO, &time,
				sizeof(struct timeval));
		}

		/*
		 * Adding the client needs both mutexes, in the usual order: readers
		 * that only take the lossy one (ws_sendframe_lossy scanning the table)
		 * must never see a slot whose locks do not exist yet, which is also
		 * why the fd is published last.
		 */
		pthread_mutex_lock(&mutex);
		pthread_mutex_lock(&lossy_mutex);
		for (i = 0; i < MAX_CLIENTS; i++)
		{
			if (cli_fd(&client_socks[i]) == -1)
			{
				if (pthread_mutex_init(&client_socks[i].mtx_state, NULL))
					panic("Error on allocating close mutex");
				if (pthread_cond_init(&client_socks[i].cnd_state_close, NULL))
					panic("Error on allocating condition var\n");
				if (pthread_mutex_init(&client_socks[i].mtx_snd, NULL))
					panic("Error on allocating send mutex");
				if (pthread_mutex_init(&client_socks[i].mtx_ping, NULL))
					panic("Error on allocating ping/pong mutex");

				client_socks[i].state = WS_STATE_CONNECTING;
				client_socks[i].close_thrd = false;
				client_socks[i].last_pong_id = -1;
				client_socks[i].current_ping_id = -1;

				/*
				 * The client table is memset(-1) at startup, so on a slot that
				 * never had an outbox the frame buffers are garbage rather
				 * than NULL. Clear them (and mark the outbox retired, which
				 * lets ws_outbox_prepare() reuse them) here, where the slot is
				 * initialised; one entry per WS_LOSSY_QUEUE_DEPTH slot.
				 */
				client_socks[i].outbox.magic = WS_OUTBOX_IDLE;
				client_socks[i].outbox.slots[0].buf = NULL;
				client_socks[i].outbox.slots[1].buf = NULL;
				client_socks[i].outbox.slots[0].alloc = 0;
				client_socks[i].outbox.slots[1].alloc = 0;

				/*
				 * Publish the fd last: nothing above this point may be
				 * missing once another thread can reach the slot through it.
				 * The owning client thread starts after both mutexes release.
				 */
				cli_fd_set(&client_socks[i], new_sock);
				set_client_address(&client_socks[i]);
				break;
			}
		}
		pthread_mutex_unlock(&lossy_mutex);
		pthread_mutex_unlock(&mutex);

		/* Client socket added to socks list ? */
		if (i != MAX_CLIENTS)
		{
			if (pthread_create(
					&client_thread, NULL, ws_establishconnection, &client_socks[i]))
				panic("Could not create the client thread!");

			pthread_detach(client_thread);
		}
		else
			close_socket(new_sock);
	}
	free(data);
	return (data);
}

/**
 * @brief Main loop for the server.
 *
 * @param evs  Events structure.
 * @param port Server port.
 * @param thread_loop If any value other than zero, runs
 *                    the accept loop in another thread
 *                    and immediately returns. If 0, runs
 *                    in the same thread and blocks execution.
 *
 * @param timeout_ms  Max timeout if the client is not responding
 *                    (in milliseconds).
 *
 * @return If @p thread_loop != 0, returns 0. Otherwise, never
 * returns.
 */
int ws_socket(struct ws_events *evs, uint16_t port, int thread_loop,
	uint32_t timeout_ms)
{
	struct sockaddr_in server; /* Server.                */
	pthread_t accept_thread;   /* Accept thread.         */
	int reuse;                 /* Socket option.         */
	int *sock;                 /* Client sock.           */

	timeout = timeout_ms;

	/* Ignore 'unused functions' warnings. */
	((void)skip_frame);

	/* Checks if the event list is a valid pointer. */
	if (evs == NULL)
		panic("Invalid event list!");

	/* Allocates our sock data. */
	sock = malloc(sizeof(*sock));
	if (!sock)
		panic("Unable to allocate sock, out of memory!\n");

	/* Copy events. */
	memcpy(&cli_events, evs, sizeof(struct ws_events));

#ifdef _WIN32
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
		panic("WSAStartup failed!");

	/**
	 * Sets stdout to be non-buffered.
	 *
	 * According to the docs from MSDN (setvbuf page), Windows do not
	 * really supports line buffering but full-buffering instead.
	 *
	 * Quote from the docs:
	 * "... _IOLBF For some systems, this provides line buffering.
	 *  However, for Win32, the behavior is the same as _IOFBF"
	 */
	setvbuf(stdout, NULL, _IONBF, 0);
#endif

	/* Create socket. */
	*sock = socket(AF_INET, SOCK_STREAM, 0);
	if (*sock < 0)
		panic("Could not create socket");

	/* Reuse previous address. */
	reuse = 1;
	if (setsockopt(*sock, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse,
			sizeof(reuse)) < 0)
	{
		panic("setsockopt(SO_REUSEADDR) failed");
	}

	/* Prepare the sockaddr_in structure. */
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(port);

	/* Bind. */
	if (bind(*sock, (struct sockaddr *)&server, sizeof(server)) < 0)
		panic("Bind failed");

	/* Listen. */
	listen(*sock, MAX_CLIENTS);

	/* Wait for incoming connections. */
	printf("Waiting for incoming connections...\n");
	memset(client_socks, -1, sizeof(client_socks));

	/* Accept connections. */
	if (!thread_loop)
		ws_accept((void *)sock);
	else
	{
		if (pthread_create(&accept_thread, NULL, ws_accept, (void *)sock))
			panic("Could not create the client thread!");
		pthread_detach(accept_thread);
	}

	return (0);
}

#ifdef AFL_FUZZ
/**
 * @brief WebSocket fuzzy test routine
 *
 * @param evs  Events structure.
 *
 * @param file File to be read.
 *
 * @return Returns 0, or crash.
 *
 * @note This is a 'fuzzy version' of the function @ref ws_socket.
 * This routine do not listen to any port nor accept multiples
 * connections. It is intended to read a stream of frames through a
 * file and process it as if they are coming from a socket.
 *
 * This behavior enables us to test wsServer against fuzzers, like
 * AFL, and see if it crashes, hangs or behaves normally, even under
 * weird conditions.
 */
int ws_file(struct ws_events *evs, const char *file)
{
	int sock;
	sock = open(file, O_RDONLY);
	if (sock < 0)
		panic("Invalid file\n");

	/* Copy events. */
	memcpy(&cli_events, evs, sizeof(struct ws_events));

	/* Clear client socks list. */
	memset(client_socks, -1, sizeof(client_socks));

	/* Set client settings. */
	client_socks[0].client_sock = sock;
	client_socks[0].state = WS_STATE_CONNECTING;
	client_socks[0].close_thrd = false;

	/* Initialize mutexes. */
	if (pthread_mutex_init(&client_socks[0].mtx_state, NULL))
		panic("Error on allocating close mutex");
	if (pthread_cond_init(&client_socks[0].cnd_state_close, NULL))
		panic("Error on allocating condition var\n");
	if (pthread_mutex_init(&client_socks[0].mtx_snd, NULL))
		panic("Error on allocating send mutex");
	if (pthread_mutex_init(&client_socks[0].mtx_ping, NULL))
		panic("Error on allocating ping/pong mutex");

	ws_establishconnection(&client_socks[0]);
	return (0);
}
#endif
