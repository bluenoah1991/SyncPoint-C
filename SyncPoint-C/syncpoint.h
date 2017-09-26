#ifndef SYNCPOINT_DEFINE_
#define SYNCPOINT_DEFINE_

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "sqlite3.h"

#include "cpack.h"

// result codes
#define SP_OK               0   /* Successful result */
#define SP_ERROR            1   /* Generic error */

// STRUCT

typedef struct sp_point{
    uint64_t sync_number;
    cp_buf *data;
} sp_point;

typedef struct sp_request_object{
    uint64_t id;
    uint64_t client_sn;
    cp_array *new_points;
} sp_request_object;

typedef struct sp_response_object{
    uint64_t new_sn;
    uint64_t id;
    uint64_t sn;
    cp_array *points;
} sp_response_object;

typedef struct sp_client_options{
    const char *scope;
    const char *db_path;
} sp_client_options;

typedef struct sp_client{
    sp_client_options *opts;
    cp_client *cp;
    sqlite3 *db;
    uint64_t sync_id;
    uint64_t number;
    uint64_t anonymous_number;
    uint64_t need_sync_sn;
    void(*implement_handle)(cp_buf *data, void *p);
    void(*resolving_reverse_conflicts)(
        cp_array *anonymous_points, cp_array *points, uint64_t sn,
        cp_array **local_conflicts_points, cp_array **new_points, void *p);
    void *implement_handle_obj;
    void *resolving_reverse_conflicts_obj;

    // serialize and deserialize
    int (*serialize)(const sp_request_object *obj, cp_buf **buf);
    int (*deserialize)(const cp_buf *buf, sp_response_object *obj);
} sp_client;

// SP_POINT

void *sp_point_copy(const sp_point *src_point);

// Communication

int sp_parse_body(sp_client *client, const cp_buf *body);

int sp_generate_body(sp_client *client, cp_buf **body);

// Interface

int sp_client_init(sp_client **client, sp_client_options *opts);;

void sp_client_free(sp_client **client);

void sp_register_serialize(sp_client *client, 
    int (*fn)(const sp_request_object *obj, cp_buf **buf));

void sp_register_deserialize(sp_client *client, 
    int (*fn)(const cp_buf *buf, sp_response_object *obj));

void sp_register_implement_handle(sp_client *client, 
    void(*fn)(cp_buf *data, void *p), void *p);

void sp_register_resolving_reverse_conflicts(sp_client *client, 
    void(*fn)(
    cp_array *anonymous_points, cp_array *points, uint64_t sn,
    cp_array **local_conflicts_points, cp_array **new_points, void *p), void *p);

int sp_start_sync(sp_client *client, int *is_sync);

int sp_add_point(sp_client *client, const cp_buf *data);

#endif
