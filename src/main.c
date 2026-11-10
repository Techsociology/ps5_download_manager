#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <microhttpd.h>
#include "dlmgr.h"

struct MHD_Daemon *srv_start(unsigned short port);
void srv_stop(struct MHD_Daemon *d);
volatile int g_quit, g_quit_ui;
static volatile sig_atomic_t g_resume;
static void on_cont(int s) { (void)s; g_resume = 1; }

int main(void) {
    plat_kill_previous_instance();
    plat_init();
    signal(SIGPIPE, SIG_IGN); signal(SIGCONT, on_cont);
    log_msg("[dlmgr] Download Manager v%s starting on port %d\n", DL_VERSION, DL_PORT);
    eng_init();

    Settings s; eng_settings_get(&s);
    if (s.auto_install_app) plat_install_app();

    struct MHD_Daemon *d = srv_start(DL_PORT);
    if (!d) { plat_notify("Download Manager: port %d is busy", DL_PORT); eng_shutdown(); return 1; }

    char ip[64] = "unknown"; plat_local_ip(ip, sizeof ip);
    plat_notify("Download Manager v%s\nhttp://%s:%d", DL_VERSION, ip, DL_PORT);

    /* Watchdog: the listening socket dies when the console sleeps or the IP changes, so restart it. */
    int tick = 0;
    while (!g_quit) {
        usleep(100000);
        int restart = 0;
        if (g_resume) { g_resume = 0; usleep(1000000); restart = 1; }
        if (++tick >= 50) {
            tick = 0; char n[64] = "unknown"; plat_local_ip(n, sizeof n);
            if (strcmp(n, ip)) { log_msg("[dlmgr] network %s -> %s\n", ip, n); snprintf(ip, sizeof ip, "%s", n); restart = 1; }
        }
        if (restart) {
            srv_stop(d); usleep(500000); d = srv_start(DL_PORT);
            if (!d) { log_msg("[dlmgr] server restart failed\n"); plat_notify("Download Manager: server restart failed"); usleep(2000000); d = srv_start(DL_PORT); if (!d) break; }
        }
    }
    log_msg("[dlmgr] shutting down\n");
    if (g_quit_ui) usleep(700000);                       /* let the browser receive the reply of the Exit button */
    srv_stop(d); eng_shutdown();
    return 0;
}
