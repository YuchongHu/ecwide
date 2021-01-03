#include "common.hpp"

//pthread_mutex_t mutex;
//LEVEL is high, show less
void vlog(int l, const char *fmt, ...)
{
    if (LEVEL <= l)
    {
        va_list ap;
        char msg[1024];

        va_start(ap, fmt);
        vsnprintf(msg, sizeof(msg), fmt, ap);
        va_end(ap);

        fprintf(stdout, "%s", msg);
    }
    //pthread_mutex_unlock(&mutex);
}

void print_err(const char *str, int err_no)
{
    printf("%s :%s\n", str, strerror(err_no));
    //_exit(-1);
}

int offset(int g, int r, int index_tag, int gid_self)
{
    if (g > gid_self)
        return (g - 1) * RACK * NODE + r * NODE + index_tag;
    else if (g < gid_self)
        return g * RACK * NODE + r * NODE + index_tag;
}

double timeval_diff(struct timeval *start, struct timeval *end)
{
    double r = (end->tv_sec - start->tv_sec) * 1000000.0;

    if (end->tv_usec > start->tv_usec)
        r += (end->tv_usec - start->tv_usec);
    else if (end->tv_usec < start->tv_usec)
        r -= (start->tv_usec - end->tv_usec);
    return r;
}