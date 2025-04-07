# redis-rqe
Redis JSON with Redis Query Engine - fake data &amp; simple performance testing

# Setup

- Use Python virtual env.
- Tested with Python 3.13 (April 2025)

# Notes

By default each thread opens its own connection
Defaults to 20 threads and 2M entries which amounts for 1.22GB of dataset (700 byte on average)

# Loading data (insert/update)

This will display overall stats for the pipeline call (default 100)
The below reads as 6500 ops/s (mean_rate) at 0.51ms per entry (average)
```
./.venv/bin/python refaker.py
== 2025-04-07 20:36:23 ===================================
pipeline:
                 avg = 0.05085065275206602
                 sum = 67.6313681602478
               count = 1330.0
                 max = 0.29595494270324707
                 min = 0.0066530704498291016
             std_dev = 0.026725395284827026
            15m_rate = 65.4667788422879
             5m_rate = 65.46896290272728
             1m_rate = 65.48294253480452
           mean_rate = 65.513759033191
       50_percentile = 0.045761942863464355
       75_percentile = 0.06119662523269653
       95_percentile = 0.09101370573043821
       99_percentile = 0.13109859943389907
      999_percentile = 0.29521874594688424
0.5101557597173564
```
