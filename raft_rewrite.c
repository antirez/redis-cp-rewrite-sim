#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define NUM_INSTANCES 5
#define SAVE_TIME 3
#define MAX_LOG_LEN 10000000

struct instance {
    int leader;
    long long rewrite_started;
    int log_len;
    int max_len;
};

struct instance instances[NUM_INSTANCES];

/* Set the log len to +/- 25% of MAX_LOG_LEN. */
void set_max_log_len(struct instance *i) {
    i->max_len = MAX_LOG_LEN + ((rand() % MAX_LOG_LEN/2)-MAX_LOG_LEN/4);
}

void init(void) {
    int j;

    for (j = 0; j < NUM_INSTANCES; j++) {
        instances[j].leader = j == 0;
        instances[j].rewrite_started = 0;
        instances[j].log_len = 0;
        set_max_log_len(instances+j);
    }
}

int main(void) {
    long long time = 0, nomajority = 0;
    init();

    while(1) {
        time++; /* Every increment is 1/millionth of second. */

        /* Step 1: Increment log len (we simulate 10k writes per second) */
        if ((time % 100) == 0) {
            for (int j = 0; j < NUM_INSTANCES; j++) {
                instances[j].log_len++;
            }
        }

        /* Step 2: Trigger rewrite in followers. */
        for (int j = 0; j < NUM_INSTANCES; j++) {
            if (instances[j].leader) continue;
            if (instances[j].rewrite_started) {
                long long elapsed = time - instances[j].rewrite_started;
                if (elapsed > SAVE_TIME * 1000000) {
                    instances[j].rewrite_started = 0;
                    instances[j].log_len = 1;
                    set_max_log_len(instances+j);
                    printf("Rewrite terminated for %d\n", j);
                }
                continue;
            }

            if (instances[j].log_len < instances[j].max_len) continue;

            /* Ok this instance may rewrite. */
            instances[j].rewrite_started = time;
            printf("Rewrite started for %d\n", j);
        }

        /* Step 3: Leader needs rewrite?  We check every 60 seconds. */
        int leader_needs_rewrite = -1;
        if (!(time % 1000000*60)) {
            for (int j = 0; j < NUM_INSTANCES; j++) {
                if (instances[j].leader &&
                    instances[j].log_len > instances[j].max_len)
                {
                    leader_needs_rewrite = j;
                    printf("Leader %d needs write\n",j);
                }
            }
        }

        /* Trigger failover. */
        if (leader_needs_rewrite != -1) {
            int follower;
            while(1) {
                follower = rand() % NUM_INSTANCES;
                if (follower != leader_needs_rewrite) break;
            }

            if (instances[follower].log_len < instances[follower].max_len &&
                instances[follower].rewrite_started == 0)
            {
                instances[follower].leader = 1;
                instances[leader_needs_rewrite].leader = 0;
                printf("%d failed over\n", follower);
            }
        }

        /* Compute unavailability window. */
        int majority = 0;
        for (int j = 0; j < NUM_INSTANCES; j++) {
            if (instances[j].rewrite_started == 0) majority++;
        }
        if (majority < NUM_INSTANCES/2+1) nomajority++;

        /* Log some info about instances. */
        if (!(time % 1000000)) {
            for (int j = 0; j < NUM_INSTANCES; j++) {
                printf("Instance %d\n", j);
                printf("log_len: %d\n", instances[j].log_len);
                printf("rewrite_started: %lld\n", instances[j].rewrite_started);
                printf("\n");
            }

            printf("Not available %lld on a total of %lld time\n",
                nomajority, time);
        }
    }
}
