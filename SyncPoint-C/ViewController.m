//
//  ViewController.m
//  SyncPoint-C
//
//  Created by codemeow on 22/09/2017.
//  Copyright © 2017 codemeow. All rights reserved.
//

#import "ViewController.h"

#import "syncpoint.h"

@implementation ViewController{
    sp_client *sp;
    
    NSString *scope;
}

static NSString *outgoingUrl = @"http://127.0.0.1:8080/outgoing";
static NSString *incomingUrl = @"http://127.0.0.1:8080/incoming";

- (void)startLongPollingRequest{
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 100 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
        NSURL *url = [NSURL URLWithString:outgoingUrl];
        NSURLRequest *request = [NSURLRequest requestWithURL:url cachePolicy:NSURLRequestReloadIgnoringLocalCacheData timeoutInterval:0];
        NSMutableURLRequest *mutableRequest = [request mutableCopy];
        [mutableRequest addValue:scope forHTTPHeaderField:@"SyncPoint-Scope"];
        [mutableRequest setHTTPMethod:@"POST"];
        request = [mutableRequest copy];
        NSURLSession *urlSession = [NSURLSession sessionWithConfiguration:
                                    [NSURLSessionConfiguration defaultSessionConfiguration] delegate:self delegateQueue:[NSOperationQueue currentQueue]];
        NSURLSessionDataTask *dataTask = [urlSession dataTaskWithRequest:request];
        [dataTask resume];
    });
}

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask didReceiveData:(NSData *)data{
    cp_buf *body = cp_buf_init();
    cp_buf_append(body, [data bytes], [data length]);
    sp_parse_body(sp, body);
    cp_buf_free(body);
}

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask willCacheResponse:(NSCachedURLResponse *)proposedResponse completionHandler:(void (^)(NSCachedURLResponse * _Nullable))completionHandler{
    [session finishTasksAndInvalidate];
    [self startLongPollingRequest];
}

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error{
    [session finishTasksAndInvalidate];
    [self startLongPollingRequest];
}

- (void)startRequest{
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 100 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
        NSURL *url = [NSURL URLWithString:incomingUrl];
        NSURLRequest *request = [NSURLRequest requestWithURL:url cachePolicy:NSURLRequestReloadIgnoringLocalCacheData timeoutInterval:0];
        NSMutableURLRequest *mutableRequest = [request mutableCopy];
        [mutableRequest addValue:@"application/octet-stream" forHTTPHeaderField:@"Content-Type"];
        [mutableRequest addValue:scope forHTTPHeaderField:@"SyncPoint-Scope"];
        [mutableRequest setHTTPMethod:@"POST"];
        // Set HTTP body
        cp_buf *body;
        int rc = sp_generate_body(sp, &body);
        if(rc){
            [self startRequest];
            return;
        }
        if(body->size == 0){
            [self startRequest];
            return;
        }
        NSData *bodyData = [NSData dataWithBytes:body->data length:body->size];
        cp_buf_free(body);
        [mutableRequest setHTTPBody:bodyData];
        request = [mutableRequest copy];
        NSURLSession *urlSession = [NSURLSession sharedSession];
        NSURLSessionDataTask *dataTask = [urlSession dataTaskWithRequest:request completionHandler:^(NSData * _Nullable data, NSURLResponse * _Nullable response, NSError * _Nullable error) {
            [self startRequest];
        }];
        [dataTask resume];
    });
}

static int sp_serialize(const sp_request_object *obj, cp_buf **buf){
    NSMutableArray *points_ = [[NSMutableArray alloc]init];
    for(int i = 0; i < obj->new_points->size; i++){
        sp_point *point = obj->new_points->p[i];
        NSData *pointData = [NSData dataWithBytes:point->data->data length:point->data->size];
        NSString *pointDataStr = [[NSString alloc]initWithData:pointData encoding:NSUTF8StringEncoding];
        NSDictionary *pointDic = [[NSDictionary alloc]initWithObjectsAndKeys:
                                  pointDataStr, @"data",
                                  [NSNumber numberWithUnsignedLong:point->sync_number], @"syncNumber", nil];
        [points_ addObject:pointDic];
    }
    NSArray *points = [points_ copy];
    NSDictionary *mapData = [[NSDictionary alloc]initWithObjectsAndKeys:
                             [NSNumber numberWithUnsignedLong:obj->id], @"id",
                             [NSNumber numberWithUnsignedLong:obj->client_sn], @"clientNumberOfSegment",
                             points, @"newPoints", nil];
    NSData *data = [NSJSONSerialization dataWithJSONObject:mapData options:0 error:nil];
    *buf = cp_buf_init();
    cp_buf_append(*buf, [data bytes], [data length]);
    return 0;
}

static int sp_deserialize(const cp_buf *buf, sp_response_object *obj){
    NSData *data = [NSData dataWithBytes:buf->data length:buf->size];
    NSDictionary *jsonObj = (NSDictionary *)[NSJSONSerialization JSONObjectWithData:data options:0 error:nil];
    if([jsonObj objectForKey:@"newNumberOfSegment"] != nil){
        obj->new_sn = [(NSNumber *)[jsonObj objectForKey:@"newNumberOfSegment"] unsignedLongValue];
    } else {
        obj->id = [(NSNumber *)[jsonObj objectForKey:@"id"] unsignedLongValue];
        obj->sn = [(NSNumber *)[jsonObj objectForKey:@"numberOfSegment"] unsignedLongValue];
        NSArray *arr = (NSArray *)[jsonObj objectForKey:@"points"];
        cp_array *points = cp_array_init();
        for(id item in arr){
            NSDictionary *pointDic = (NSDictionary *)item;
            sp_point *point;
            point = malloc(sizeof(*point));
            cp_buf *data = cp_buf_init();
            NSString *pointDataStr = (NSString *)[pointDic objectForKey:@"data"];
            NSData *pointData = [pointDataStr dataUsingEncoding:NSUTF8StringEncoding];
            cp_buf_append(data, [pointData bytes], [pointData length]);
            point->data = data;
            point->sync_number = (uint64_t)[pointDic objectForKey:@"syncNumber"];
            cp_array_push(points, point);
        }
        obj->points = points;
    }
    return 0;
}

static void implement_handle(cp_buf *data, void *p){
    ViewController *vc = (__bridge ViewController *)(p);
    NSData *dataObj = [NSData dataWithBytes:data->data length:data->size];
    NSDictionary *pointDic = (NSDictionary *)[NSJSONSerialization JSONObjectWithData:dataObj options:0 error:nil];
    NSString *name = [pointDic objectForKey:@"id"];
    NSString *value = [pointDic objectForKey:@"value"];
    NSTextField *textField = [vc valueForKey:name];
    [textField setStringValue:value];
}

static void resolving_reverse_conflicts(cp_array *anonymous_points, cp_array *points, uint64_t sn, cp_array **local_conflicts_points, cp_array **new_points, void *p){
    *local_conflicts_points = cp_array_init();
    *new_points = cp_array_init();
    for(int i = 0; i < points->size; i++){
        sp_point *point = points->p[i];
        NSData *pointData = [NSData dataWithBytes:point->data->data length:point->data->size];
        NSDictionary *pointDic = (NSDictionary *)[NSJSONSerialization JSONObjectWithData:pointData options:0 error:nil];
        NSString *pointName = [pointDic objectForKey:@"id"];
        bool is_conflicts = false;
        for(int j = 0; j < anonymous_points->size; j++){
            sp_point *anonymous_point = anonymous_points->p[j];
            NSData *anonymousPointData = [NSData dataWithBytes:anonymous_point->data->data length:anonymous_point->data->size];
            NSDictionary *anonymousPointDic = (NSDictionary *)[NSJSONSerialization JSONObjectWithData:anonymousPointData options:0 error:nil];
            NSString *anonymousPointName = [anonymousPointDic objectForKey:@"id"];
            if([anonymousPointName isEqualToString:pointName]){
                is_conflicts = true;
                break;
            }
        }
        if(!is_conflicts){
            sp_point *point_copy = sp_point_copy(point);
            cp_array_push(*local_conflicts_points, point_copy);
        }
    }
    for(int i = 0; i < anonymous_points->size; i++){
        sp_point *anonymous_point = sp_point_copy(anonymous_points->p[i]);
        anonymous_point->sync_number = ++sn;
        cp_array_push(*new_points, anonymous_point);
    }
}

- (void)viewDidLoad {
    [super viewDidLoad];

    // Do any additional setup after loading the view.

    [_startButton setTarget:self];
    [_startButton setAction:@selector(start:)];
    [_clearButton setTarget:self];
    [_clearButton setAction:@selector(clear:)];
    [_clearButton setEnabled:NO];
}

- (void)start:(id)sender{
    NSString *scope_ = [_usernameTextField stringValue];
    if([scope_ length] == 0){
        return;
    }
    scope = scope_;
    [_startButton setEnabled:NO];
    sp_client_options *opts;
    opts = malloc(sizeof(*opts));
    opts->scope = (char *)[scope cStringUsingEncoding:NSUTF8StringEncoding];
    NSString *dbPath = [NSString stringWithFormat:@"sp-%@.db", scope_];
    opts->db_path = [dbPath cStringUsingEncoding:NSUTF8StringEncoding];
    sp_client_init(&sp, opts);
    sp_register_serialize(sp, sp_serialize);
    sp_register_deserialize(sp, sp_deserialize);
    sp_register_implement_handle(sp, implement_handle, (__bridge void *)(self));
    sp_register_resolving_reverse_conflicts(sp, resolving_reverse_conflicts, (__bridge void *)(self));
    [self startLongPollingRequest];
    [self startRequest];
    sp_start_sync(sp, NULL);
    
    for (int i = 1; i <= 12; i++) {
        NSTextField *textField = [self valueForKey:[NSString stringWithFormat:@"_textField%d", i]];
        [textField setTarget:self];
        [textField setAction:@selector(textChanged:)];
    }
    
    [_clearButton setEnabled:YES];
}

- (void)clear:(id)sender{
    if(sp == NULL){
        return;
    }
    sp_client_free(&sp);
    [_clearButton setEnabled:NO];
}

- (void)textChanged:(id)sender{
    NSTextField *textField = (NSTextField *)sender;
    if([[textField stringValue]length] == 0){
        return;
    }
    for (int i = 1; i <= 12; i++) {
        NSString *name = [NSString stringWithFormat:@"_textField%d", i];
        NSTextField *textField_ = [self valueForKey:name];
        if(textField == textField_){
            NSDictionary *pointDic = [[NSDictionary alloc]initWithObjectsAndKeys:name, @"id", [textField stringValue], @"value", nil];
            NSData *pointData = [NSJSONSerialization dataWithJSONObject:pointDic options:0 error:nil];
            cp_buf *buf = cp_buf_init();
            cp_buf_append(buf, [pointData bytes], [pointData length]);
            sp_add_point(sp, buf);
        }
    }
}


- (void)setRepresentedObject:(id)representedObject {
    [super setRepresentedObject:representedObject];

    // Update the view, if already loaded.
}


@end











