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

/**
 * @dir include/
 * @brief wsServer include directory
 *
 * @file ws.h
 * @brief wsServer constants and functions.
 */
#ifndef WS_H
#define WS_H

#ifdef __cplusplus
extern "C" {
#endif

	#include <stdbool.h>
	#include <stdint.h>
	#include <inttypes.h>

	/**
	 * @name Global configurations
	 */
	/**@{*/
	/**
	 * @brief Max clients connected simultaneously.
	 */
#ifndef MAX_CLIENTS
	#define MAX_CLIENTS    8
#endif

	/**
	 * @name Key and message configurations.
	 */
	/**@{*/
	/**
	 * @brief Message buffer length.
	 */
	#define MESSAGE_LENGTH 2048
	/**
	 * @brief Maximum frame/message length.
	 */
	#define MAX_FRAME_LENGTH (16*1024*1024)
	/**
	 * @brief WebSocket key length.
	 */
	#define WS_KEY_LEN     24
	/**
	 * @brief Magic string length.
	 */
	#define WS_MS_LEN      36
	/**
	 * @brief Accept message response length.
	 */
	#define WS_KEYMS_LEN   (WS_KEY_LEN + WS_MS_LEN)
	/**
	 * @brief Magic string.
	 */
	#define MAGIC_STRING   "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	/**@}*/

	/**
	 * @name Handshake constants.
	 */
	/**@{*/
	/**
	 * @brief Alias for 'Sec-WebSocket-Key'.
	 */
	#define WS_HS_REQ      "Sec-WebSocket-Key"

	/**
	 * @brief Handshake accept message length.
	 */
	#define WS_HS_ACCLEN   130

	/**
	 * @brief Handshake accept message.
	 */
	#define WS_HS_ACCEPT                       \
		"HTTP/1.1 101 Switching Protocols\r\n" \
		"Upgrade: websocket\r\n"               \
		"Connection: Upgrade\r\n"              \
		"Sec-WebSocket-Accept: "
	/**@}*/

	/**
	 * @name Frame types.
	 */
	/**@{*/
	/**
	 * @brief Frame FIN.
	 */
	#define WS_FIN      128

	/**
	 * @brief Frame FIN shift
	 */
	#define WS_FIN_SHIFT  7

	/**
	 * @brief Continuation frame.
	 */
	#define WS_FR_OP_CONT 0

	/**
	 * @brief Text frame.
	 */
	#define WS_FR_OP_TXT  1

	/**
	 * @brief Binary frame.
	 */
	#define WS_FR_OP_BIN  2

	/**
	 * @brief Close frame.
	 */
	#define WS_FR_OP_CLSE 8

	/**
	 * @brief Ping frame.
	 */
	#define WS_FR_OP_PING 0x9

	/**
	 * @brief Pong frame.
	 */
	#define WS_FR_OP_PONG 0xA

	/**
	 * @brief Unsupported frame.
	 */
	#define WS_FR_OP_UNSUPPORTED 0xF
	/**@}*/

	/**
	 * @name Close codes
	 */
	/**@{*/
	/**
	 * @brief Normal close
	 */
	#define WS_CLSE_NORMAL  1000
	/**
	 * @brief Protocol error
	 */
	#define WS_CLSE_PROTERR 1002
	/**@}*/
	/**
	 * @brief Inconsistent message (invalid utf-8)
	 */
	#define WS_CLSE_INVUTF8 1007

	/**
	 * @name Connection states
	 */
	/**@{*/
	/**
	 * @brief Connection not established yet.
	 */
	#define WS_STATE_CONNECTING 0
	/**
	 * @brief Communicating.
	 */
	#define WS_STATE_OPEN       1
	/**
	 * @brief Closing state.
	 */
	#define WS_STATE_CLOSING    2
	/**
	 * @brief Closed.
	 */
	#define WS_STATE_CLOSED     3
	/**@}*/

	/**
	 * @name Timeout util
	 */
	/**@{*/
	/**
	 * @brief Nanoseconds macro converter
	 */
	#define MS_TO_NS(x) ((x)*1000000)
	/**
	 * @brief Timeout in milliseconds.
	 */
	#define TIMEOUT_MS (500)
	/**@}*/

	/**
	 * @name Handshake constants.
	 */
	/**@{*/
	/**
	 * @brief Debug
	 */
	#ifdef VERBOSE_MODE
	#define DEBUG(...) fprintf(stderr, __VA_ARGS__)
	#else
	#define DEBUG(...)
	#endif
	/**@}*/

	/**
	 * @name Lossy output (see ws_sendframe_lossy)
	 */
	/**@{*/
	/**
	 * @brief Largest payload a single lossy frame may carry. Bigger frames
	 * are rejected instead of buffered.
	 */
	#define WS_LOSSY_MAX_FRAME (2 * 1024 * 1024)

	/**
	 * @brief Outbox depth: one frame held by the sender thread while it
	 * writes, plus one holding the newest frame not yet started.
	 */
	#define WS_LOSSY_QUEUE_DEPTH 2

	/**
	 * @brief Outbox frame buffers up to this size survive a disconnect and
	 * are reused by the next client of the same slot, so that streaming does
	 * not allocate on every frame.
	 */
	#define WS_LOSSY_KEEP_ALLOC (512 * 1024)

	/**
	 * @brief How long (ms) a client may keep refusing data before its
	 * connection is aborted.
	 */
	#define WS_LOSSY_STALL_MS 5000

	/**
	 * @brief Send threshold used for TCP_NOTSENT_LOWAT when available.
	 */
	#define WS_LOSSY_NOTSENT_LOWAT (32 * 1024)
	/**@}*/

	/**
	 * @brief Reads the socket fd of a client.
	 *
	 * The fd is published and reset only under the library locks, but it is
	 * read from several threads without any (validity checks, send/recv), so
	 * the access is atomic: a reader can then never see a half-written value.
	 */
	#define WS_CLI_FD(client) \
		__atomic_load_n(&(client)->client_sock, __ATOMIC_ACQUIRE)

	#ifndef AFL_FUZZ
	#define SEND(client,buf,len) send_all((client), (buf), (len), MSG_NOSIGNAL)
	#define RECV(fd,buf,len) recv(WS_CLI_FD(fd), (buf), (len), 0)
	#else
	#define SEND(client,buf,len) write(fileno(stdout), (buf), (len))
	#define RECV(fd,buf,len) read(WS_CLI_FD(fd), (buf), (len))
	#endif

	/* Opaque client connection type. */
	typedef struct ws_connection ws_cli_conn_t;

	/**
	 * @brief events Web Socket events types.
	 */
	struct ws_events
	{
		/**
		 * @brief On open event, called when a new client connects.
		 */
		void (*onopen)(ws_cli_conn_t *client);

		/**
		 * @brief On close event, called when a client disconnects.
		 */
		void (*onclose)(ws_cli_conn_t *client);

		/**
		 * @brief On message event, called when a client sends a text
		 * or binary message.
		 */
		void (*onmessage)(ws_cli_conn_t *client,
			const unsigned char *msg, uint64_t msg_size, int type);
	};

	/* Forward declarations. */

	/* Internal usage. */
	extern int get_handshake_accept(char *wsKey, unsigned char **dest);
	extern int get_handshake_response(char *hsrequest, char **hsresponse);

	/* External usage. */
	extern char *ws_getaddress(ws_cli_conn_t *client);
	extern int ws_sendframe(
		ws_cli_conn_t *cli, const char *msg, uint64_t size, int type);
	extern int ws_sendframe_txt(ws_cli_conn_t *cli, const char *msg);
	extern int ws_sendframe_bin(ws_cli_conn_t *cli, const char *msg, uint64_t size);

	/*
	 * Lossy, non-blocking send for latency sensitive streams (video, audio).
	 *
	 * The frame is copied into a one-slot outbox per client and written by a
	 * per-client sender thread, so the caller never waits on a slow client.
	 * If a client still has a frame queued, the queued frame is replaced by
	 * the new one (the newest frame always wins) and @p behind, when given,
	 * is set to 1 so the caller can react - e.g. force the next H.264 frame
	 * to be a key frame.
	 *
	 * A client that stops accepting data for WS_LOSSY_STALL_MS, or whose
	 * socket errors mid-frame, is disconnected: partial frames never stay on
	 * the wire. As the outbox keeps only the newest frame, a client can miss
	 * frames in normal operation, which is why the caller is told about it.
	 *
	 * @param cli    Client to send to; NULL broadcasts to all open clients.
	 * @param msg    Payload, copied before returning.
	 * @param size   Payload size, at most WS_LOSSY_MAX_FRAME.
	 * @param type   Frame opcode, e.g. WS_FR_OP_BIN.
	 * @param behind Optional out flag, cleared by the callee.
	 *
	 * @return The number of clients the frame was queued for, 0 if there is
	 * nobody to send to, or -1 on error (invalid client, oversized frame or
	 * out of memory).
	 */
	extern int ws_sendframe_lossy(ws_cli_conn_t *cli, const char *msg, uint64_t size,
		int type, int *behind);
	extern int ws_sendframe_bin_lossy(ws_cli_conn_t *cli, const char *msg, uint64_t size,
		int *behind);
	extern int ws_get_state(ws_cli_conn_t *cli);
	extern int ws_close_client(ws_cli_conn_t *cli);
	extern int ws_socket(struct ws_events *evs, uint16_t port, int thread_loop,
		uint32_t timeout_ms);

	/* Ping routines. */
	extern void ws_ping(ws_cli_conn_t *cli, int threshold);

#ifdef AFL_FUZZ
	extern int ws_file(struct ws_events *evs, const char *file);
#endif

#ifdef __cplusplus
}
#endif

#endif /* WS_H */
