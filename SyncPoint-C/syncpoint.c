#include "syncpoint.h"

// Function Define

uint64_t _sp_number_of_segment(uint64_t number);

uint64_t _sp_number_of_next_segment(uint64_t number);

void _sp_point_free(void *p);

int _sp_update_sync_id(sp_client *client, uint64_t sync_id);

int _sp_add_anonymous_point(sp_client *client, uint64_t serial_number, const cp_buf *data);

int _sp_add_point(sp_client *client, uint64_t number, const cp_buf *data);

int _sp_add_points(sp_client *client, cp_array *points);

int _sp_points_at_range(sp_client *client, uint64_t sn, uint64_t snn, cp_array **points);

int _sp_dump_anonymous_points(sp_client *client, cp_array **points);

int _sp_clear_anonymous_points(sp_client *client);

void _sp_parse_body_callback(const cp_buf *payload, void *p);

// Common Function

uint64_t _sp_number_of_segment(uint64_t number){
    uint64_t sn = number >> 16;
    return sn << 16;
}

uint64_t _sp_number_of_next_segment(uint64_t number){
    uint64_t sn = (number >> 16) + 1;
    return sn << 16;
}

void _sp_point_free(void *p){
    sp_point *point = p;
    cp_buf_free(point->data);
    free(point);
}

// Storage

int _sp_update_sync_id(sp_client *client, uint64_t sync_id){
    char *sql = "INSERT OR REPLACE INTO SP_SYNCID (SCOPE, SYNCID) VALUES (\"client\", ?);";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 1, sync_id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    sqlite3_finalize(stmt);
    return CP_OK;
}

int _sp_add_anonymous_point(sp_client *client, uint64_t serial_number, const cp_buf *data){
    char *sql = "INSERT INTO SP_ANONYMOUS_POINTS " \
        "(SERIALNUMBER, DATA) VALUES (?, ?);";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 1, serial_number);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    if(data == NULL){
        rc = sqlite3_bind_null(stmt, 2);
    } else {
        rc = sqlite3_bind_blob(stmt, 2, data->data, (int)data->size, NULL);
    }
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    sqlite3_finalize(stmt);
    return SP_OK;
}

int _sp_add_point(sp_client *client, uint64_t number, const cp_buf *data){
    char *sql = "INSERT INTO SP_POINTS " \
        "(NUMBER, DATA) VALUES (?, ?);";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 1, number);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    if(data == NULL){
        rc = sqlite3_bind_null(stmt, 2);
    } else {
        rc = sqlite3_bind_blob(stmt, 2, data->data, (int)data->size, NULL);
    }
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    sqlite3_finalize(stmt);
    return SP_OK;
}

int _sp_add_points(sp_client *client, cp_array *points){
    int rc;
    for(int i = 0; i < points->size; i++){
        sp_point *point = points->p[i];
        rc = _sp_add_point(client, point->sync_number, point->data);
        if(rc){
            return SP_ERROR;
        }
    }
    return SP_OK;
}
    
int _sp_points_at_range(sp_client *client, uint64_t sn, uint64_t snn, cp_array **points){
    *points = cp_array_init();
    char *sql = "SELECT * FROM SP_POINTS WHERE NUMBER >= ? AND NUMBER < ? ORDER BY NUMBER ASC;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 1, sn);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 2, snn);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
        uint64_t number = 0;
        cp_buf *data = cp_buf_init();
        int columns = sqlite3_column_count(stmt);
        for(int i = 0; i < columns; i++){
            const char *name = sqlite3_column_name(stmt, i);
            if(strcmp("NUMBER", name) == 0){
                number = (uint64_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("DATA", name) == 0){
                size_t size = sqlite3_column_bytes(stmt, i);
                const void *d = sqlite3_column_blob(stmt, i);
                cp_buf_append(data, d, size);
            }
        }
        sp_point *point;
        point = malloc(sizeof(*point));
        point->sync_number = number;
        point->data = data;
        cp_array_push(*points, point);
    }
    return SP_OK;
}

int _sp_dump_anonymous_points(sp_client *client, cp_array **points){
    *points = cp_array_init();
    char *sql = "SELECT * FROM SP_ANONYMOUS_POINTS ORDER BY SERIALNUMBER ASC;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
        uint64_t serial_number;
        cp_buf *data = cp_buf_init();
        int columns = sqlite3_column_count(stmt);
        for(int i = 0; i < columns; i++){
            const char *name = sqlite3_column_name(stmt, i);
            if(strcmp("SERIALNUMBER", name) == 0){
                serial_number = (uint64_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("DATA", name) == 0){
                size_t size = sqlite3_column_bytes(stmt, i);
                const void *d = sqlite3_column_blob(stmt, i);
                cp_buf_append(data, d, size);
            }
        }
        sp_point *point;
        point = malloc(sizeof(*point));
        point->sync_number = -1;
        point->data = data;
        cp_array_push(*points, point);
    }
    return SP_OK;
}

int _sp_clear_anonymous_points(sp_client *client){
    char *sql = "DELETE FROM SP_ANONYMOUS_POINTS WHERE 1 = 1;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    sqlite3_finalize(stmt);
    return SP_OK;
}

// SP_POINT

void *sp_point_copy(const sp_point *src_point){
    sp_point *point;
    point = malloc(sizeof(*point));
    point->sync_number = src_point->sync_number;
    cp_buf *data = cp_buf_copy(src_point->data);
    point->data = data;
    return point;
}

// Communication

void _sp_parse_body_callback(const cp_buf *payload, void *p){
    sp_client *client = p;
    sp_response_object obj;
    memset(&obj, 0, sizeof(obj));
    int rc = client->deserialize(payload, &obj);
    if(rc){
        return;
    }
    if(obj.new_sn){
        uint64_t sn = _sp_number_of_segment(client->number);
        if(obj.new_sn > sn){
            int is_sync;
            rc = sp_start_sync(client, &is_sync);
            if(rc){
                return;
            }
            if(!is_sync && client->need_sync_sn < obj.new_sn){
                client->need_sync_sn = obj.new_sn;
            }
        }
    } else {
        if(obj.id - client->sync_id != 1){
            return;
        }
        bool need_sync = false;
        if(client->resolving_reverse_conflicts != NULL){
            cp_array *anonymous_points;
            rc = _sp_dump_anonymous_points(client, &anonymous_points);
            if(rc){
                cp_array_free(obj.points, _sp_point_free);
                return;
            }
            cp_array *local_conflicts_points;
            cp_array *new_points;
            client->resolving_reverse_conflicts(
                anonymous_points, obj.points, obj.sn,
                &local_conflicts_points, &new_points, client->resolving_reverse_conflicts_obj);
            cp_array_free(anonymous_points, _sp_point_free);
            rc = _sp_clear_anonymous_points(client);
            if(rc){
                cp_array_free(obj.points, _sp_point_free);
                return;
            }
            if(client->implement_handle != NULL){
                for(int i = 0; i < local_conflicts_points->size; i++){
                    sp_point *point = local_conflicts_points->p[i];
                    client->implement_handle(point->data, client->implement_handle_obj);
                }
            }
            cp_array_free(local_conflicts_points, _sp_point_free);
            rc = _sp_add_points(client, obj.points);
            cp_array_free(obj.points, _sp_point_free);
            obj.points = NULL;
            if(rc){
                return;
            }
            rc = _sp_add_points(client, new_points);
            if(rc){
                return;
            }
            client->number = obj.sn;
            if(new_points->size > 0){
                need_sync = true;
            }
            cp_array_free(new_points, _sp_point_free);
        }
        client->sync_id = obj.id;
        rc = _sp_update_sync_id(client, client->sync_id);
        if(rc){
            return;
        }
        if(need_sync || client->need_sync_sn > obj.sn){
            sp_start_sync(client, NULL);
        }
    }
}

int sp_parse_body(sp_client *client, const cp_buf *body){
    if(client == NULL){
        return SP_ERROR;
    }
    return cp_parse_body(client->cp, body, _sp_parse_body_callback, client);
}

int sp_generate_body(sp_client *client, cp_buf **body){
    if(client == NULL){
        return SP_ERROR;
    }
    return cp_generate_body(client->cp, body);
}

// Interface

int sp_client_init(sp_client **client, sp_client_options *opts){
    int rc;
    char *err;
    sqlite3 *db;

    rc = sqlite3_open(opts->db_path, &db);
    if(rc != SQLITE_OK){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return SP_ERROR;
    }

    // Create database structure
    char *sql = 
                "CREATE TABLE IF NOT EXISTS SP_SYNCID(" \
                "SCOPE          TEXT PRIMARY KEY    NOT NULL," \
                "SYNCID         INTEGER             NOT NULL" \
                "); " \
                "CREATE TABLE IF NOT EXISTS SP_POINTS(" \
                "NUMBER         INTEGER PRIMARY KEY     NOT NULL," \
                "DATA           BLOB" \
                "); " \
                "CREATE TABLE IF NOT EXISTS SP_ANONYMOUS_POINTS(" \
                "SERIALNUMBER   INTEGER PRIMARY KEY     NOT NULL," \
                "DATA           BLOB" \
                ");";
    rc = sqlite3_exec(db, sql, NULL, 0, &err);
    if(rc != SQLITE_OK){
        fprintf(stderr, "SQL error: %s\n", err);
        sqlite3_free(err);
        return SP_ERROR;
    }

    *client = malloc(sizeof(**client));
    (*client)->opts = opts;
    // Install CP_CLIENT
    cp_client *cp;
    rc = cp_client_init(&cp, opts->db_path);
    if(rc){
        free(*client);
        return SP_ERROR;
    }
    (*client)->cp = cp;
    (*client)->need_sync_sn = 0;
    (*client)->implement_handle = NULL;
    (*client)->implement_handle_obj = NULL;
    (*client)->resolving_reverse_conflicts = NULL;
    (*client)->resolving_reverse_conflicts_obj = NULL;
    sqlite3_stmt *stmt;

    // Fill sync id into SP_CLIENT
    sql = "SELECT SYNCID FROM SP_SYNCID WHERE SCOPE = \"client\" LIMIT 1;";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        free(*client);
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc == SQLITE_ROW){
        const uint64_t sync_id = sqlite3_column_int64(stmt, 0);
        (*client)->sync_id = sync_id;
    } else if(rc == SQLITE_DONE){
        (*client)->sync_id = 0;
    } else {
        free(*client);
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }

    sqlite3_finalize(stmt);

    // Fill number into SP_CLIENT
    sql = "SELECT MAX(NUMBER) FROM SP_POINTS LIMIT 1;";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        free(*client);
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc == SQLITE_ROW){
        const uint64_t number = sqlite3_column_int64(stmt, 0);
        (*client)->number = number;
    } else if(rc == SQLITE_DONE){
        (*client)->number = 0;
    } else {
        free(*client);
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }

    sqlite3_finalize(stmt);

    // Fill anonymous number into SP_CLIENT
    sql = "SELECT MAX(SERIALNUMBER) FROM SP_ANONYMOUS_POINTS LIMIT 1;";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        free(*client);
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc == SQLITE_ROW){
        const uint64_t serial_number = sqlite3_column_int64(stmt, 0);
        (*client)->anonymous_number = serial_number;
    } else if(rc == SQLITE_DONE){
        (*client)->anonymous_number = 0;
    } else {
        free(*client);
        sqlite3_finalize(stmt);
        return SP_ERROR;
    }

    sqlite3_finalize(stmt);

    (*client)->db = db;
    return SP_OK;
}

void sp_client_free(sp_client **client){
    cp_client_free(&((*client)->cp));
    sqlite3_close((*client)->db);
    free((*client)->opts);
    free((*client));
    *client = NULL;
}

void sp_register_serialize(sp_client *client, 
    int (*fn)(const sp_request_object *obj, cp_buf **buf)){
    if(client == NULL){
        return;
    }
    client->serialize = fn;
}

void sp_register_deserialize(sp_client *client, 
    int (*fn)(const cp_buf *buf, sp_response_object *obj)){
    if(client == NULL){
        return;
    }
    client->deserialize = fn;
}

void sp_register_implement_handle(sp_client *client, 
    void(*fn)(cp_buf *data, void *p), void *p){
    if(client == NULL){
        return;
    }
    client->implement_handle = fn;
    client->implement_handle_obj = p;
}

void sp_register_resolving_reverse_conflicts(sp_client *client, 
    void(*fn)(
    cp_array *anonymous_points, cp_array *points, uint64_t sn,
    cp_array **local_conflicts_points, cp_array **new_points, void *p), void *p){
    if(client == NULL){
        return;
    }
    client->resolving_reverse_conflicts = fn;
    client->resolving_reverse_conflicts_obj = p;
}

int sp_start_sync(sp_client *client, int *is_sync){
    if(client == NULL){
        return SP_ERROR;
    }
    if(client->sync_id % 2 == 1){
        if(is_sync != NULL){
            *is_sync = 0;
        }
        return SP_OK;
    }
    int rc = _sp_update_sync_id(client, ++(client->sync_id));
    if(rc){
        return SP_ERROR;
    }
    uint64_t sn = _sp_number_of_segment(client->number);
    uint64_t snn = _sp_number_of_next_segment(client->number);
    cp_array *points;
    rc = _sp_points_at_range(client, sn, snn, &points);
    if(rc){
        return SP_ERROR;
    }
    sp_request_object obj;
    obj.id = client->sync_id;
    obj.client_sn = sn;
    obj.new_points = points;
    cp_buf *data;
    rc = client->serialize(&obj, &data);
    cp_array_free(points, _sp_point_free);
    if(rc){
        return SP_ERROR;
    }
    rc = cp_commit_packet(client->cp, data, 2);
    cp_buf_free(data);
    if(rc){
        return SP_ERROR;
    }
    if(is_sync != NULL){
        *is_sync = 1;
    }
    return SP_OK;
}

int sp_add_point(sp_client *client, const cp_buf *data){
    if(client == NULL){
        return SP_ERROR;
    }
    int rc;
    if(client->sync_id % 2 == 1){
        rc = _sp_add_anonymous_point(client, ++(client->anonymous_number), data);
        if(rc){
            return SP_ERROR;
        }
    } else {
        rc = _sp_add_point(client, ++(client->number), data);
        if(rc){
            return SP_ERROR;
        }
        return sp_start_sync(client, NULL);
    }
    return SP_OK;
}
