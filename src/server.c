/* HTTP server (libmicrohttpd) on port 5837: web UI + JSON API. */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <microhttpd.h>
#include "dlmgr.h"
#include "assets_index_html.h"

#define BODY_MAX (512 * 1024)
typedef struct { char *buf; size_t len; } Req;

static enum MHD_Result reply(struct MHD_Connection *c, unsigned code, const char *type, void *data, size_t len, int gz, int must_free) {
    struct MHD_Response *r = MHD_create_response_from_buffer(len, data, must_free ? MHD_RESPMEM_MUST_FREE : MHD_RESPMEM_PERSISTENT);
    MHD_add_response_header(r, "Content-Type", type);
    MHD_add_response_header(r, "Cache-Control", "no-store");
    MHD_add_response_header(r, "X-Content-Type-Options", "nosniff");
    if (gz) MHD_add_response_header(r, "Content-Encoding", "gzip");
    enum MHD_Result ret = MHD_queue_response(c, code, r);
    MHD_destroy_response(r);
    return ret;
}
static enum MHD_Result json(struct MHD_Connection *c, unsigned code, char *s) { return reply(c, code, "application/json", s, strlen(s), 0, 1); }
static enum MHD_Result jerr(struct MHD_Connection *c, unsigned code, const char *msg) {
    char b[400], esc[300]; size_t o = 0;
    for (const char *p = msg; *p && o < sizeof esc - 3; p++) { if (*p == '"' || *p == '\\') esc[o++] = ' '; else if ((unsigned char)*p >= 32) esc[o++] = *p; }
    esc[o] = 0; snprintf(b, sizeof b, "{\"ok\":false,\"error\":\"%s\"}", esc);
    return json(c, code, strdup(b));
}
static const char *arg(struct MHD_Connection *c, const char *k) { return MHD_lookup_connection_value(c, MHD_GET_ARGUMENT_KIND, k); }
static int iarg(struct MHD_Connection *c, const char *k, int dflt) { const char *v = arg(c, k); return v && *v ? atoi(v) : dflt; }

static enum MHD_Result handle(void *cls, struct MHD_Connection *c, const char *url, const char *method,
                              const char *ver, const char *up, size_t *upn, void **con) {
    (void)cls; (void)ver;
    if (!*con) { *con = calloc(1, sizeof(Req)); return *con ? MHD_YES : MHD_NO; }
    Req *rq = *con;
    if (*upn) {                                                   /* accumulate POST body */
        if (rq->len + *upn < BODY_MAX) {
            rq->buf = realloc(rq->buf, rq->len + *upn + 1);
            memcpy(rq->buf + rq->len, up, *upn); rq->len += *upn; rq->buf[rq->len] = 0;
        }
        *upn = 0; return MHD_YES;
    }
    int is_get = !strcmp(method, "GET") || !strcmp(method, "HEAD"), is_post = !strcmp(method, "POST");
    if (!is_get && !is_post) return jerr(c, 405, "Method not allowed");

    if (is_get && (!strcmp(url, "/") || !strcmp(url, "/index.html")))
        return reply(c, 200, "text/html; charset=utf-8", (void *)assets_index_html, assets_index_html_len, 1, 0);

    if (!strcmp(url, "/add")) {                                   /* bookmarklet / shortcut entry: GET /add?url=..&t=TOKEN */
        const char *t = arg(c, "t"), *u = arg(c, "url"); char err[200] = "";
        if (!t || strcmp(t, eng_token())) return reply(c, 403, "text/plain", "Bad or missing token. Copy the bookmarklet from the Download Manager page.", 70, 0, 0);
        if (arg(c, "ask") && u && *u) {                           /* ask=1: open the UI with the Add dialog pre-filled so the user picks the queue/folder */
            char loc[2600]; size_t o = (size_t)snprintf(loc, sizeof loc, "/?add="), n = strlen(u);
            for (size_t i = 0; i < n && o + 4 < sizeof loc; i++) {
                unsigned char ch = (unsigned char)u[i];
                if (isalnum(ch) || strchr("-_.~", ch)) loc[o++] = (char)ch; else o += (size_t)snprintf(loc + o, 4, "%%%02X", ch);
            }
            loc[o] = 0;
            struct MHD_Response *r = MHD_create_response_from_buffer(0, "", MHD_RESPMEM_PERSISTENT);
            MHD_add_response_header(r, "Location", loc);
            enum MHD_Result ret = MHD_queue_response(c, 302, r); MHD_destroy_response(r); return ret;
        }
        int id = eng_add(u, arg(c, "name"), arg(c, "dir"), iarg(c, "q", 1), err, sizeof err, NULL);
        struct MHD_Response *r = MHD_create_response_from_buffer(0, "", MHD_RESPMEM_PERSISTENT);
        MHD_add_response_header(r, "Location", id > 0 ? "/" : "/?error=1");
        enum MHD_Result ret = MHD_queue_response(c, 302, r); MHD_destroy_response(r); return ret;
    }

    if (strncmp(url, "/api/", 5)) return reply(c, 404, "text/plain", "Not found", 9, 0, 0);
    const char *ep = url + 5;

    if (is_get && !strcmp(ep, "state")) return json(c, 200, eng_state_json());
    if (is_get && !strcmp(ep, "ls")) { char e2[200] = ""; char *r = eng_ls_json(arg(c, "path"), e2, sizeof e2); return r ? json(c, 200, r) : jerr(c, 400, e2); }

    /* every mutating call needs a custom header: a cross-site page cannot send it without a CORS preflight, which we never grant */
    if (!MHD_lookup_connection_value(c, MHD_HEADER_KIND, "X-DM")) return jerr(c, 403, "Missing X-DM header");

    char err[200] = "", out[160];
    if (!strcmp(ep, "add")) {
        int dup = 0, id = eng_add(arg(c, "url"), arg(c, "name"), arg(c, "dir"), iarg(c, "q", 1), err, sizeof err, &dup);
        if (id < 0) return jerr(c, 400, err);
        snprintf(out, sizeof out, "{\"ok\":true,\"id\":%d,\"dup\":%d}", id, dup); return json(c, 200, strdup(out));
    }
    if (!strcmp(ep, "mkdir")) { char np[800]; if (eng_mkdir(arg(c, "path"), arg(c, "name"), np, sizeof np, err, sizeof err)) return jerr(c, 400, err);
        char *o = malloc(1000); snprintf(o, 1000, "{\"ok\":true,\"path\":\"%s\"}", np); return json(c, 200, o); }
    if (!strcmp(ep, "scan")) { char *r = eng_scan(arg(c, "url"), err, sizeof err); return r ? json(c, 200, r) : jerr(c, 400, err); }
    if (!strcmp(ep, "bulk")) {                                    /* POST body: one link per line, optional "url|name" */
        int added = 0, bad = 0, dups = 0; int q = iarg(c, "q", 1); const char *dir = arg(c, "dir");
        char *body = rq->buf ? strdup(rq->buf) : NULL, *save = NULL;
        for (char *ln = body ? strtok_r(body, "\r\n", &save) : NULL; ln; ln = strtok_r(NULL, "\r\n", &save)) {
            while (*ln == ' ' || *ln == '\t') ln++;
            if (!*ln || *ln == '#') continue;
            char *bar = strchr(ln, '|'), *nm = NULL; if (bar) { *bar = 0; nm = bar + 1; }
            int dp = 0; if (eng_add(ln, nm, dir, q, err, sizeof err, &dp) > 0) { if (dp == 1) dups++; else added++; } else bad++;
        }
        free(body);
        snprintf(out, sizeof out, "{\"ok\":true,\"added\":%d,\"rejected\":%d,\"dups\":%d}", added, bad, dups); return json(c, 200, strdup(out));
    }
    if (!strcmp(ep, "item")) {
        int rc = eng_item_action(iarg(c, "id", -1), arg(c, "act"), arg(c, "arg"));
        return rc ? jerr(c, 400, "Action not possible in the item's current state") : json(c, 200, strdup("{\"ok\":true}"));
    }
    if (!strcmp(ep, "queue")) {
        const char *act = arg(c, "act") ? arg(c, "act") : ""; int rc = -1, id = iarg(c, "id", -1);
        if (!strcmp(act, "create")) { id = eng_queue_create(arg(c, "name")); rc = id > 0 ? 0 : -1; }
        else if (!strcmp(act, "set")) rc = eng_queue_update(id, arg(c, "name"), iarg(c, "max", -1), iarg(c, "running", -1), iarg(c, "limit", -1), iarg(c, "chain", -1));
        else if (!strcmp(act, "sched")) rc = eng_queue_schedule(id, iarg(c, "on", 0), iarg(c, "start", 0), iarg(c, "stop", 0), iarg(c, "tz", 0));
        else if (!strcmp(act, "delete")) rc = eng_queue_delete(id);
        if (rc) return jerr(c, 400, "Queue operation failed");
        snprintf(out, sizeof out, "{\"ok\":true,\"id\":%d}", id); return json(c, 200, strdup(out));
    }
    if (!strcmp(ep, "settings")) {
        Settings s; eng_settings_get(&s);
        s.segments = iarg(c, "segments", s.segments); s.limit_kbps = iarg(c, "limit_kbps", s.limit_kbps);
        s.retries = iarg(c, "retries", s.retries); s.auto_install_app = iarg(c, "auto_install_app", s.auto_install_app); s.notify = iarg(c, "notify", s.notify);
        if (arg(c, "dir") && *arg(c, "dir")) snprintf(s.dir, sizeof s.dir, "%s", arg(c, "dir"));
        if (eng_settings_set(&s, err, sizeof err)) return jerr(c, 400, err);
        return json(c, 200, strdup("{\"ok\":true}"));
    }
    if (!strcmp(ep, "shutdown")) { extern volatile int g_quit, g_quit_ui; g_quit_ui = 1; g_quit = 1; return json(c, 200, strdup("{\"ok\":true}")); }
    return jerr(c, 404, "Unknown endpoint");
}

static void completed(void *cls, struct MHD_Connection *c, void **con, enum MHD_RequestTerminationCode t) {
    (void)cls; (void)c; (void)t; Req *r = *con; if (r) { free(r->buf); free(r); } *con = NULL;
}

struct MHD_Daemon *srv_start(unsigned short port) {
    return MHD_start_daemon(MHD_USE_THREAD_PER_CONNECTION, port, NULL, NULL, handle, NULL,
                            MHD_OPTION_NOTIFY_COMPLETED, completed, NULL,
                            MHD_OPTION_THREAD_STACK_SIZE, (size_t)(1024 * 1024),
                            MHD_OPTION_CONNECTION_LIMIT, 48u, MHD_OPTION_CONNECTION_TIMEOUT, 30u, MHD_OPTION_END);
}
void srv_stop(struct MHD_Daemon *d) { if (d) MHD_stop_daemon(d); }
