#include "cpack.h"

#define CP_P_UNIX 1
#define CP_P_WINDOWS 2

#if defined(__unix__) || defined(__APPLE__)
#define CP_PLATFORM CP_P_UNIX
#elif defined(_WIN32)
#define CP_PLATFORM CP_P_WINDOWS
#endif

#if CP_PLATFORM == CP_P_UNIX
#include <unistd.h>
#elif CP_PLATFORM == CP_P_WINDOWS
#include <windows.h>
#endif

void cp_sleep(int ms){
#if CP_PLATFORM == CP_P_UNIX
    // usleep takes sleep time in us (1 millionth of a second)
    usleep(ms * 1000);   
#elif CP_PLATFORM == CP_P_WINDOWS
    Sleep(ms);
#endif
}

// Fuction Define

int _cp_packet_compare(node nd1, node nd2);

void _cp_packet_free(node nd);

int _cp_generate_id(cp_client *client, uint16_t *id);

int _cp_save_packet(cp_client *client, cp_packet *packet);

int _cp_remove_packet(cp_client *client, uint16_t id);

int _cp_update_packet(cp_client *client, cp_packet *packet);

int _cp_next_packet(cp_packet **r_packet, cp_packet *packet);

int _cp_push_packet(cp_client *client, cp_packet *packet);

int _cp_pop_packet(cp_client *client, uint16_t id);

int _cp_receive_message(cp_client *client, uint16_t id, cp_buf *payload);

int _cp_delete_message(cp_client *client, uint16_t id);

int _cp_release_message(cp_client *client, uint16_t id, cp_buf **payload);

int _cp_unconfirmed_packet(cp_client *client, cp_array **packets, size_t limit);

int _cp_confirm_packet(cp_client *client, uint16_t id);

int _cp_combine_packets(const cp_array *packets, cp_buf **buf);

int _cp_split_packets(const cp_buf *buf, cp_array **packets);

int _cp_handle_packet(cp_client *client, const cp_packet *packet, 
    void(*callback)(const cp_buf *payload, void *p), void *p);

// CP_BUF

void *cp_buf_init(){
    cp_buf *buf;
    buf = (cp_buf *)malloc(sizeof(*buf));
    buf->data = NULL;
    buf->size = 0;
    return buf;
};

void cp_buf_append(cp_buf *buf, const char *data, size_t size){
    if(size){
        if(buf->size){
            buf->data = realloc(buf->data, buf->size + size);
        } else {
            buf->data = malloc(size);
        }
        memcpy(buf->data + buf->size, data, size);
        buf->size += size;
    }
}

void *cp_buf_copy(const cp_buf *src_buf){
    char *data = NULL;
    if(src_buf->size){
        data = malloc(src_buf->size);
        memcpy(data, src_buf->data, src_buf->size);
    }
    cp_buf *dst_buf;
    dst_buf = malloc(sizeof(*dst_buf));
    dst_buf->data = data;
    dst_buf->size = src_buf->size;
    return dst_buf;
}

void cp_buf_free(cp_buf *buf){
    if(buf != NULL){
        free(buf->data);
    }
    free(buf);
}

void cp_buf_to_ch(const cp_buf *buf, char **ch){
    *ch = malloc(buf->size + 1);
    memcpy(*ch, buf->data, buf->size);
    (*ch)[buf->size] = '\0';
}

// CP_ARRAY

void *cp_array_init(){
    cp_array *arr;
    arr = (cp_array *)malloc(sizeof(*arr));
    arr->p = NULL;
    arr->size = 0;
    return arr;
}

void cp_array_push(cp_array *arr, void *p){
    if(arr->size){
        arr->p = realloc(arr->p, sizeof(*(arr->p)) * ++(arr->size));
    } else {
        arr->p = malloc(sizeof(*(arr->p)) * ++(arr->size));
    }
    arr->p[arr->size - 1] = p;
}

void cp_array_free(cp_array *arr, void(*release)(void *p)){
    for(int i = 0; i < arr->size; i++){
        release(arr->p[i]);
    }
    free(arr);
}

// PACK AND UNPACK (BIG ENDIAN)

uint8_t read_byte(char **pptr){
    uint8_t c = **pptr;
    (*pptr)++;
    return c;
}

uint16_t read_short(char **pptr){
    char* ptr = *pptr;
    uint16_t i = ((uint8_t)(*ptr)) << 8;
    i += (uint8_t)(*(ptr+1));
    *pptr += 2;
    return i;
}

void *read_data(char **pptr, uint32_t length){
    void *data = malloc(length);
    memcpy(data, *pptr, length);
    *pptr += length;
    return data;
}

void write_byte(char **pptr, uint8_t b){
    **pptr = b;
    (*pptr)++;
}

void write_short(char **pptr, uint16_t i){
    **pptr = (char)(i >> 8);
    (*pptr)++;
    **pptr = (char)i;
    (*pptr)++;
}

void write_data(char **pptr, const void *data, uint32_t length){
    memcpy(*pptr, data, length);
    *pptr += length;
}

// PROTOCOL

void *cp_encode_packet(
    uint8_t type, uint8_t qos, bool dup, uint16_t id, 
    const cp_buf *payload){
    uint32_t remaining_length = payload == NULL ? 0 : payload->size;

    char *data = malloc(5 + remaining_length);
    char *ptr = data;
    char fixed_header = (type << 4) | (qos << 2) | (dup << 1);
    write_byte(&ptr, fixed_header);
    write_short(&ptr, id);
    write_short(&ptr, remaining_length);
    if(payload != NULL){
        write_data(&ptr, payload->data, payload->size);
    }

    cp_buf *buf = cp_buf_init();
    cp_buf_append(buf, data, 5 + remaining_length);

    free(data);

    cp_packet *packet;
    packet = malloc(sizeof(*packet));
    packet->type = type;
    packet->qos = qos;
    packet->dup = dup;
    packet->id = id;
    packet->remaining_length = remaining_length;
    packet->total_length = 5 + remaining_length;
    cp_buf *payload2 = NULL;
    if(payload != NULL){
        payload2 = cp_buf_copy(payload);
    }
    packet->payload = payload2;
    packet->buffer = buf;
    packet->timestamp = 0;
    packet->retry_times = 0;
    return packet;
}

void *cp_decode_packet(const cp_buf *buffer, size_t offset){
    char *buffer_data = buffer->data;
    char *ptr = buffer_data + offset;

    uint8_t fixed_header = read_byte(&ptr);
    uint8_t type = fixed_header >> 4;
    uint8_t qos = (fixed_header & 0xf) >> 2;
    bool dup = (fixed_header & 0x3) >> 1;
    uint16_t id = read_short(&ptr);
    uint16_t remaining_length = read_short(&ptr);

    char *data = (char *)read_data(&ptr, remaining_length);
    cp_buf *payload = cp_buf_init();
    cp_buf_append(payload, data, remaining_length);

    cp_packet *packet;
    packet = malloc(sizeof(*packet));
    packet->type = type;
    packet->qos = qos;
    packet->dup = dup;
    packet->id = id;
    packet->remaining_length = remaining_length;
    packet->total_length = 5 + remaining_length;
    packet->payload = payload;
    packet->buffer = cp_buf_copy(buffer);
    packet->timestamp = 0;
    packet->retry_times = 0;
    return packet;
}

void cp_packet_free(void *p){
    cp_packet *packet = p;
    cp_buf_free(packet->payload);
    cp_buf_free(packet->buffer);
    free(packet);
}

// INTERFACE

int _cp_packet_compare(node nd1, node nd2){
    cp_packet *packet1 = nd1;
    cp_packet *packet2 = nd2;
    return packet1->timestamp < packet2->timestamp;
}

void _cp_packet_free(node nd){
    cp_packet *packet = nd;
    cp_packet_free(packet);
}

int _cp_generate_id(cp_client *client, uint16_t *id){
    char *sql = "INSERT OR REPLACE INTO CP_IDENTIFIER (SCOPE, IDENTIFIER) VALUES (\"client\", ?);";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 1, (client->nid + 1));
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    sqlite3_finalize(stmt);
    *id = ++(client->nid);
    return CP_OK;
}

int _cp_save_packet(cp_client *client, cp_packet *packet){
    char *sql = "INSERT INTO CP_PACKETS " \
        "(ID, TYPE, QOS, DUP, REMAININGLENGTH, PAYLOAD, TIMESTAMP, RETRYTIMES) " \
        "VALUES " \
        "(?, ?, ?, ?, ?, ?, ?, ?);";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 1, packet->id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 2, packet->type);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 3, packet->qos);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 4, packet->dup);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 5, packet->remaining_length);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    if(packet->payload == NULL){
        rc = sqlite3_bind_null(stmt, 6);
    } else {
        rc = sqlite3_bind_blob(stmt, 6, packet->payload->data, packet->payload->size, NULL);
    }
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 7, packet->timestamp);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 8, packet->retry_times);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    sqlite3_finalize(stmt);
    return CP_OK;
}

int _cp_remove_packet(cp_client *client, uint16_t id){
    char *sql = "DELETE FROM CP_PACKETS WHERE ID = ?;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 1, id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    sqlite3_finalize(stmt);
    return CP_OK;
}

int _cp_update_packet(cp_client *client, cp_packet *packet){
    char *sql = "UPDATE CP_PACKETS SET TIMESTAMP = ?, RETRYTIMES = ? WHERE ID = ?;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int64(stmt, 1, packet->timestamp);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 2, packet->retry_times);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 3, packet->id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    sqlite3_finalize(stmt);
    return CP_OK;
}

int _cp_next_packet(cp_packet **r_packet, cp_packet *packet){
    *r_packet = malloc(sizeof(**r_packet));
    **r_packet = *packet;
    (*r_packet)->retry_times++;
    (*r_packet)->timestamp += (*r_packet)->retry_times * 5;
    if(packet->payload == NULL){
        (*r_packet)->payload = NULL;
    } else {
        (*r_packet)->payload = cp_buf_copy(packet->payload);
    } 
    (*r_packet)->buffer = cp_buf_copy(packet->buffer);
    return CP_OK;
}

int _cp_push_packet(cp_client *client, cp_packet *packet){
    int rc = _cp_save_packet(client, packet);
    if(rc){
        return rc;
    }
    heap_insert_node(client->packets, packet);
    return CP_OK;
}

int _cp_pop_packet(cp_client *client, uint16_t id){
    for(int i = 0; i < client->packets->size; i++){
        cp_packet *packet = client->packets->elem[i];
        if(packet->id == id){
            int rc = _cp_remove_packet(client, id);
            if(rc){
                return CP_ERROR;
            }
            heap_delete_node(client->packets, i);
        }
    }
    return CP_OK;
}

int _cp_receive_message(cp_client *client, uint16_t id, cp_buf *payload){
    char *sql = "INSERT INTO CP_LIVING_MESSAGES " \
        "(ID, PAYLOAD) VALUES (?, ?)";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 1, id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_blob(stmt, 2, payload->data, payload->size, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    return CP_OK;
}

int _cp_delete_message(cp_client *client, uint16_t id){
    char *sql = "DELETE FROM CP_LIVING_MESSAGES WHERE ID = ?;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 1, id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_OK && rc != SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    sqlite3_finalize(stmt);
    return CP_OK;
}

int _cp_release_message(cp_client *client, uint16_t id, cp_buf **payload){
    char *sql = "SELECT PAYLOAD FROM CP_LIVING_MESSAGES WHERE ID = ? LIMIT 1;";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(client->db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_bind_int(stmt, 1, id);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    *payload = NULL;
    rc = sqlite3_step(stmt);
    if(rc == SQLITE_ROW){
        size_t size = sqlite3_column_bytes(stmt, 0);
        const char *data = sqlite3_column_blob(stmt, 0);
        *payload = cp_buf_init();
        cp_buf_append(*payload, data, size);
        rc = _cp_delete_message(client, id);
        sqlite3_finalize(stmt);
        if(rc){
            return CP_ERROR;
        }
        return CP_OK;
    } else if(rc == SQLITE_DONE){
        sqlite3_finalize(stmt);
        return CP_OK;
    } else {
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
}

int _cp_unconfirmed_packet(
    cp_client *client, cp_array **packets, size_t limit){
    int rc;
    *packets = cp_array_init();
    cp_array *r_packets = cp_array_init();
    size_t i = 0;
    while(i < limit){
        if(!client->packets->size){
            break;
        }
        cp_packet *packet = client->packets->elem[0];
        heap_delete_node(client->packets, 0);
        cp_array_push(*packets, packet);

        // Retry packet
        if(packet->qos == CP_PROTOCOL_QOS0){
            rc = _cp_remove_packet(client, packet->id);
            if(rc){
                cp_array_free(*packets, cp_packet_free);
                return CP_ERROR;
            }
        } else {
            cp_packet *r_packet;
            rc = _cp_next_packet(&r_packet, packet);
            if(rc){
                cp_array_free(*packets, cp_packet_free);
                return CP_ERROR;
            }
            rc = _cp_update_packet(client, packet);
            if(rc){
                cp_array_free(*packets, cp_packet_free);
                return CP_ERROR;
            }
            cp_array_push(r_packets, r_packet);
        }
    }

    // Fill retry packets back to heap
    if(r_packets->size){
        heap_build_heap(client->packets, r_packets->p, r_packets->size);
        free(r_packets);
    }
    return CP_OK;
}

int _cp_confirm_packet(cp_client *client, uint16_t id){
    return _cp_pop_packet(client, id);
}

int _cp_combine_packets(const cp_array *packets, cp_buf **buf){
    *buf = cp_buf_init();
    int offset = 0;
    for(int i = 0; i < packets->size; i++){
        const cp_packet *packet = packets->p[i];
        cp_buf_append(*buf, packet->buffer->data, packet->buffer->size);
    }
    return CP_OK;
}

int _cp_split_packets(const cp_buf *buf, cp_array **packets){
    *packets = cp_array_init();
    size_t offset = 0;
    while(offset < buf->size){
        cp_packet *packet = cp_decode_packet(buf, offset);
        cp_array_push(*packets, packet);
        offset += packet->total_length;
    }
    return CP_OK;
}

int _cp_handle_packet(cp_client *client, const cp_packet *packet, 
    void(*callback)(const cp_buf *payload, void *p), void *p){
    int rc;
    if(packet->type == CP_PROTOCOL_MSG_TYPE_SEND){
        if(packet->qos == CP_PROTOCOL_QOS0){
            callback(packet->payload, p);
        } else if(packet->qos == CP_PROTOCOL_QOS1){
            cp_packet *reply_packet = cp_encode_packet(
                CP_PROTOCOL_MSG_TYPE_ACK, CP_PROTOCOL_QOS0, 0, packet->id, NULL);
            rc = _cp_push_packet(client, reply_packet);
            if(rc){
                return CP_ERROR;
            }
            callback(packet->payload, p);
        } else if(packet->qos == CP_PROTOCOL_QOS2){
            rc = _cp_receive_message(client, packet->id, packet->payload);
            if(rc){
                return CP_ERROR;
            }
            cp_packet *reply_packet = cp_encode_packet(
                CP_PROTOCOL_MSG_TYPE_RECEIVED, CP_PROTOCOL_QOS0, 0, packet->id, NULL);
            rc = _cp_push_packet(client, reply_packet);
            if(rc){
                return CP_ERROR;
            }
        }
    } else if(packet->type == CP_PROTOCOL_MSG_TYPE_ACK){
        rc = _cp_confirm_packet(client, packet->id);
        if(rc){
            return CP_ERROR;
        }
    } else if(packet->type == CP_PROTOCOL_MSG_TYPE_RECEIVED){
        rc = _cp_confirm_packet(client, packet->id);
        if(rc){
            return CP_ERROR;
        }
        cp_packet *reply_packet = cp_encode_packet(
            CP_PROTOCOL_MSG_TYPE_RELEASE, CP_PROTOCOL_QOS1, 0, packet->id, NULL);
        rc = _cp_push_packet(client, reply_packet);
        if(rc){
            return CP_ERROR;
        }
    } else if(packet->type == CP_PROTOCOL_MSG_TYPE_RELEASE){
        cp_buf *payload;
        rc = _cp_release_message(client, packet->id, &payload);
        if(rc){
            return CP_ERROR;
        }
        if(payload && payload->size){
            callback(payload, p);
        }
        cp_buf_free(payload);
        cp_packet *reply_packet = cp_encode_packet(
            CP_PROTOCOL_MSG_TYPE_COMPLETED, CP_PROTOCOL_QOS0, 0, packet->id, NULL);
        rc = _cp_push_packet(client, reply_packet);
        if(rc){
            return CP_ERROR;
        }
    } else if(packet->type == CP_PROTOCOL_MSG_TYPE_COMPLETED){
        rc = _cp_confirm_packet(client, packet->id);
        if(rc){
            return CP_ERROR;
        }
    }
    return CP_OK;
}

int cp_client_init(cp_client **client, const char *dbpath){
    int rc;
    char *err;
    sqlite3 *db;

    rc = sqlite3_open(dbpath, &db);
    if(rc != SQLITE_OK){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return CP_ERROR;
    }
    // Create database structure
    char *sql = 
                "CREATE TABLE IF NOT EXISTS CP_IDENTIFIER(" \
                "SCOPE          TEXT PRIMARY KEY    NOT NULL," \
                "IDENTIFIER     INTEGER             NOT NULL" \
                "); " \
                "CREATE TABLE IF NOT EXISTS CP_PACKETS(" \
                "ID     INTEGER PRIMARY KEY     NOT NULL," \
                "TYPE               INTEGER     NOT NULL," \
                "QOS                INTEGER     NOT NULL," \
                "DUP                INTEGER     NOT NULL," \
                "REMAININGLENGTH    INTEGER     NOT NULL," \
                "PAYLOAD            BLOB," \
                "TIMESTAMP          INTEGER     NOT NULL," \
                "RETRYTIMES         INTEGER     NOT NULL" \
                "); " \
                "CREATE TABLE IF NOT EXISTS CP_LIVING_MESSAGES(" \
                "ID     INTEGER PRIMARY KEY     NOT NULL," \
                "PAYLOAD            BLOB" \
                ");";
    rc = sqlite3_exec(db, sql, NULL, 0,&err);
    if(rc != SQLITE_OK){
        fprintf(stderr, "SQL error: %s\n", err);
        sqlite3_free(err);
        return CP_ERROR;
    }

    // Fill data into min heap
    heap *packets = heap_init(_cp_packet_compare);
    sql = "SELECT * FROM CP_PACKETS;";
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
        uint8_t type;
        uint8_t qos;
        bool dup;
        uint16_t id;
        uint16_t remaining_length;
        cp_buf *payload;
        uint64_t timestamp;
        uint32_t retry_times;
        int columns = sqlite3_column_count(stmt);
        for(int i = 0; i < columns; i++){
            const char *name = sqlite3_column_name(stmt, i);
            if(strcmp("TYPE", name) == 0){
                type = (uint8_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("QOS", name) == 0){
                qos = (uint8_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("DUP", name) == 0){
                dup = (bool)sqlite3_column_int(stmt, i);
            } else if(strcmp("ID", name) == 0){
                id = (uint16_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("REMAININGLENGTH", name) == 0){
                remaining_length = (uint16_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("PAYLOAD", name) == 0){
                payload = cp_buf_init();
                size_t size = sqlite3_column_bytes(stmt, i);
                const void *data = sqlite3_column_blob(stmt, i);
                cp_buf_append(payload, data, size);
            } else if(strcmp("TIMESTAMP", name) == 0){
                timestamp = (uint64_t)sqlite3_column_int(stmt, i);
            } else if(strcmp("RETRYTIMES", name) == 0){
                retry_times = (uint32_t)sqlite3_column_int(stmt, i);
            }
        }
        cp_packet *packet = cp_encode_packet(type, qos, dup, id, payload);
        packet->timestamp = timestamp;
        packet->retry_times = retry_times;
        heap_insert_node(packets, packet);
    }
    if(rc != SQLITE_DONE){
        heap_free(packets, _cp_packet_free);
        return CP_ERROR;
        sqlite3_finalize(stmt);
    }

    *client = malloc(sizeof(**client));
    sqlite3_finalize(stmt);
    (*client)->packets = packets;

    // Fill id into CP_CLIENT
    sql = "SELECT IDENTIFIER FROM CP_IDENTIFIER WHERE SCOPE = \"client\" LIMIT 1;";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if(rc != SQLITE_OK){
        free(*client);
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    rc = sqlite3_step(stmt);
    if(rc == SQLITE_ROW){
        const uint16_t id = sqlite3_column_int(stmt, 0);
        (*client)->nid = id;
    } else if(rc == SQLITE_DONE){
        (*client)->nid = 0;
    } else {
        heap_free(packets, _cp_packet_free);
        free(*client);
        sqlite3_finalize(stmt);
        return CP_ERROR;
    }
    sqlite3_finalize(stmt);
    
    (*client)->db = db;
    return CP_OK;
}

void cp_client_free(cp_client **client){
    sqlite3_close((*client)->db);
    heap_free((*client)->packets, _cp_packet_free);
    free(*client);
    *client = NULL;
}

int cp_generate_body(cp_client *client, cp_buf **body){
    if(client == NULL){
        return CP_ERROR;
    }
    cp_array *packets;
    int rc = _cp_unconfirmed_packet(client, &packets, 5);
    if(rc){
        return CP_ERROR;
    }

    // combine packets
    rc = _cp_combine_packets(packets, body);
    cp_array_free(packets, cp_packet_free);
    if(rc){
        return CP_ERROR;
    }
    return CP_OK;
}

int cp_parse_body(cp_client *client, const cp_buf *body, 
    void(*callback)(const cp_buf *payload, void *p), void *p){
    if(client == NULL){
        return CP_ERROR;
    }
    cp_array *packets;
    int rc = _cp_split_packets(body, &packets);
    if(rc){
        return CP_ERROR;
    }
    for(int i = 0; i < packets->size; i++){
        cp_packet *packet = packets->p[i];
        rc = _cp_handle_packet(client, packet, callback, p);
        if(rc){
            cp_array_free(packets, cp_packet_free);
            return CP_ERROR;
        }
    }

    cp_array_free(packets, cp_packet_free);
    return CP_OK;
}

int cp_commit_packet(cp_client *client, const cp_buf *payload, uint8_t qos){
    if(client == NULL){
        return CP_ERROR;
    }
    uint16_t id;
    int rc = _cp_generate_id(client, &id);
    if(rc){
        return CP_ERROR;
    }
    cp_packet *packet = cp_encode_packet(CP_PROTOCOL_MSG_TYPE_SEND, qos, 0, id, payload);
    rc = _cp_save_packet(client, packet);
    if(rc){
        return CP_ERROR;
    }
    heap_insert_node(client->packets, packet);
    return CP_OK;
}

