# Additional notes on query order and event gap handling in the correlation engine.

The `StepDefinition` class has a `max_event_gap` field that specifies the maximum allowed time gap (in seconds) between
the events in the step and the previous step. This is used to ensure that the events being correlated are temporally close enough to be relevant.
That allows to to filter out events that are too far apart in time and shall not be correlated together, which
will help to reduce size of the intermediate results and improve the performance of the correlation process.

> [!NOTE] The `max_event_gap` of the first step has no effect, since there is no previous step to compare to.
> It should be set to 0 to avoid confusion.

The DetectionRule class has a `run_frequency` field that specifies how often the rule should be executed (in seconds).

The order of the queries in the steps is heuristically optimized based on the number of events that match each query.
Therefore the oder of the queries **must not** affect the correctness of the results.

## Prefiltering based on max_event_gap, run_frequency and the event timestamps

To avoid large results sets and improve performance, the queries in steps shall be
constrained to only consider events that are within a certain time window.

The time window for a step is determined by the `max_event_gap` of the step and the `run_frequency` of the rule.

The schema for time windowing is as follows:

- For the first step, the time window is determined by the `run_frequency` of the rule as follows:
    - `[t_0, t_1] = [current_time - run_frequency, current_time]` for the first rule execution
      - `current_time` shall be saved as `first_execution_time` for the next executions
    - `[t_0, t_1] = [first_execution_time - run_frequency * n, first_execution_time - run_frequency * (n - 1)]` for the n-th execution
- For subsequent steps, the time window is determined by the `max_event_gap` of the step as follows:
    - `[t_0, t_1] = [current_time - run_frequency - Sum(max_event_gap of previous steps), current_time]` for the first rule execution
    - `[t_0, t_1] = [first_execution_time - run_frequency * n - Sum(max_event_gap of previous steps), first_execution_time - run_frequency * (n - 1) - Sum(max_event_gap of previous steps)]` for the n-th execution

Visually, this can be represented as follows:

```
[--] run_frequency
[---] max_event_gap of step 2
[----] max_event_gap of step 3
```

```
[<-PAST -------------- NOW ->]
| ### TIMELINE ############# | ### EXPLANATION ##############|
                        [--]    time window for step 1 | run_1
                    [---|--]    time window for step 2 | run_1
               [----|---|--]    time window for step 3 | run_1
                     [--]       time window for step 1 | run_2
                 [---|--]       time window for step 2 | run_2
            [----|---|--]       time window for step 3 | run_2
```

This can lead to findings that are not relevant as their order is not preserved by the
time windowing. This example demonstrates this:

```
| ### TIMELINE ############# | ### EXPLANATION ##############|
                        [--]    time window for step 1 | run_1
                         X      FINDING
                    [---|--]    time window for step 2 | run_1
                     Y          FINDING
               [----|---|--]    time window for step 3 | run_1
                          Z     FINDING
```

The step logic implies that the order is `[X, Y, Z]`, but the time windowing allow for
any order like `[Z, X, Y]` in the example above.

The immense advantage of this approach is that we are independent of the order of the queries in the steps, which allows us to optimize the order based on the number of matching events and thus improve performance.

False positives must be sorted out in a post-processing step so we
only trigger when the events are consecutive in the order of the steps
AND the time gap between the events is less than the `max_event_gap` of the step.


### MER Coverage

One run of a DetectionRule covers 1x `run_frequency` of time.
To the next run, the time window is shifted by `run_frequency` and thus covers the next `run_frequency` of time.

For the coverage, an `earliest_event_time` and a `latest_event_time` will be provided.
The CorrelationEngine shall run as many times as needed to cover the entire time range between `earliest_event_time` and `latest_event_time` with the time windows of the rule executions and log the findings into
a logfile.