# Interview Prep

Questions each milestone prepares you for, with short answers in your own words.

## M0. Setup, spikes, and the collector

**1. Why does your collector run outside Databricks?**
The feeds only show the present, so they have to be polled every minute. Running a Databricks job every minute would start serverless compute about 1,440 times a day and use up the Free Edition daily quota. A small Python process polls for free and just uploads files. Databricks processes them in hourly batches. I tested that Databricks can reach RTD, so this is a cost choice, not a network workaround.

**2. How do you avoid storing the same data twice?**
Every GTFS-Realtime feed has a header timestamp saying when it was generated. The collector remembers the last timestamp per feed and skips a snapshot if it has not changed. The timestamp is also in the file name, so a re-upload overwrites the same file instead of creating a duplicate.

**3. What happens when RTD or Databricks is down?**
Fetches retry with exponential backoff (2, 4, 8 seconds). Each feed is handled separately, so one failing feed does not stop the others. Files are saved locally before uploading. A failed upload is queued and retried next cycle. Gaps that still happen are measured by a health script and documented, not hidden.

**4. Why store raw protobuf files rather than parsed rows?**
The raw files are the source of truth and cannot be downloaded again later. If I find a parsing bug, I can rebuild every downstream table from the raw layer. That is the point of a bronze/landing layer in a medallion architecture.

**5. Why partition the files by UTC date and hour?**
Partitioning lets later jobs read only the hours they need, which matters with a compute quota. UTC avoids daylight saving problems: local time repeats an hour in November and skips one in March. Local hour-of-day is still computed later as a feature.

## M1. Collector that keeps running

**1. How do you keep a laptop-based collector reliable?**
A launchd agent (the macOS service manager) restarts it if it crashes and starts it at login. `caffeinate -w` stops idle sleep while it runs. For times the laptop is closed, a GitHub Actions workflow checks every 15 minutes whether new files have arrived in the volume. If not, it takes over for 55 minutes.

**2. Why not just run the GitHub Actions collector all the time?**
It would upload every snapshot twice and use runner time for nothing. A freshness check costs about 30 seconds and only starts collecting when the main runner has stopped delivering.

**3. If both runners upload the same snapshot, do you get duplicates?**
No. The file name is the feed name plus the feed's header timestamp, so both runners produce the same path and the second upload overwrites the first. The write is idempotent by design.

**4. How do you know if you lost data?**
A health script lists the files in the volume, reads the timestamps from the file names, and reports every gap longer than 5 minutes. Gaps are documented, never filled with made-up data.

**5. Something you found that surprised you?**
`caffeinate -i <command>` did not actually prevent sleep in my setup: the process had no sleep assertion. I checked with `pmset -g assertions` and switched to `caffeinate -w <pid>`, which ties the assertion to the collector's PID. Lesson: verify the mechanism, not just that the command ran.
