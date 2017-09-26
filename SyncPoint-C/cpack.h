#ifndef CPACK_DEFINE_
#define CPACK_DEFINE_

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "sqlite3.h"

#include "heap.h"

// message type
#define CP_PROTOCOL_MSG_TYPE_SEND 1
#define CP_PROTOCOL_MSG_TYPE_ACK 2
#define CP_PROTOCOL_MSG_TYPE_RECEIVED 3
#define CP_PROTOCOL_MSG_TYPE_RELEASE 4
#define CP_PROTOCOL_MSG_TYPE_COMPLETED 5

// qos level
#define CP_PROTOCOL_QOS0 0
#define CP_PROTOCOL_QOS1 1
#define CP_PROTOCOL_QOS2 2

// result codes
#define CP_OK               0   /* Successful result */
#define CP_ERROR            1   /* Generic error */

// Array
typedef struct cp_array{
    void **p;
    size_t size;
} cp_array;

// STRUCT

typedef struct cp_buf{
    char *data;
    size_t size;
} cp_buf;

typedef struct cp_client{
    sqlite3 *db;
    uint16_t nid;
    heap *packets;
} cp_client;

typedef struct cp_packet{
    uint16_t id;
    uint8_t type;
    uint8_t qos;
    bool dup;
    uint16_t remaining_length;
    uint16_t total_length;
    struct cp_buf *payload;
    struct cp_buf *buffer;

    // Local information
    uint64_t timestamp;
    uint32_t retry_times;
} cp_packet;

// INTERFACE

// CP_BUF 

void *cp_buf_init();

void cp_buf_append(cp_buf *buf, const char *data, size_t size);

void *cp_buf_copy(const cp_buf *src_buf);

void cp_buf_free(cp_buf *buffer);

void cp_buf_to_ch(const cp_buf *buf, char **ch);

// CP_ARRAY

void *cp_array_init();

void cp_array_push(cp_array *arr, void *p);

void cp_array_free(cp_array *arr, void(*release)(void *p));

// PACK & UNPACK

uint8_t read_byte(char **pptr);

uint16_t read_short(char **pptr);

void *read_data(char **pptr, uint32_t length);

void write_byte(char **pptr, uint8_t b);

void write_short(char **pptr, uint16_t i);

void write_data(char **pptr, const void *data, uint32_t length);

// PROTOCOL

void *cp_encode_packet(
    uint8_t type, uint8_t qos, bool dup, uint16_t id, 
    const cp_buf *payload);

void *cp_decode_packet(const cp_buf *buf, size_t offset);

void cp_packet_free(void *p);

// INTERFACE

void cp_sleep(int ms);

int cp_client_init(cp_client **client, const char *dbpath);

void cp_client_free(cp_client **client);

int cp_generate_body(cp_client *client, cp_buf **body);

int cp_parse_body(cp_client *client, const cp_buf *body, 
    void(*callback)(const cp_buf *payload, void *p), void *p);

int cp_commit_packet(cp_client *client, const cp_buf *payload, uint8_t qos);

#endif
