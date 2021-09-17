# How to contribute to K-2SO

We are excited that you would like to volunteer your time to contribute to K-2SO's success!

If you haven't already, request to be added to our developer channel on [Mattermost](https://chat.il4.dso.mil/usaf-aftac/channels/osnds-onboarding)

Here are some important resources:

  * [Open Tasks - Jira](https://jira.il4.dso.mil/secure/RapidBoard.jspa?rapidView=631&projectKey=OSNDS&view=detail&selectedIssue=OSNDS-6)  See what we are working on and when things should be done
  * [Our roadmap](https://confluence.il4.dso.mil/display/OSNDS/Open+Source+Nuclear+Detection+System) See our information radiators on current/upcoming projects

## Submitting changes

Please send a [GitLab Merge Request to K2S-0](https://gitlab.gs.mil/aftac/sdd/si/k-2so/-/merge_requests/new) with a clear list of what you've done (read more about [merge requests](https://docs.gitlab.com/ee/user/project/merge_requests/). When you send a merge request, please include examples. 

Also, please be sure to follow our coding conventions (below) and make sure all of your commits are atomic (one feature per commit).

Always write a clear log message for your commits. One-line messages are fine for small changes, but bigger changes should look like this:

    $ git commit -m "A brief summary of the commit
    > 
    > A paragraph describing what changed and its impact."

## Welcomed changes

While we are always excited to see new features come to life, there are a few features we are eager to get implemented:
  
  * Offline flat file anomaly detection
  * Improved abstraction from livestream data for anomaly detectors (allowing users to more easily provide their own algorithms)
    * This is possible now, but users must add in their own functions to the logic file
    * Ideally users would provide their own algorithms as snippets in a "/detectors/" directory
  


## Coding conventions

Start reading our code and you'll get the hang of it. We try to optimize for readability. Outside of that, here are our general rules:

  * We indent using Tab (no spaces allowed, classic "Silicon Valley")
  * We use JSON for all payloads
  * We use nanoseconds since epoch for our timestamps (unix_ns)
    * You might see a few unix_ms variables floating around, these are for a specific purpose - even for this, we just covert from unix_ms
  * We try to always put paragraphs of comments before major sections of logic to help the reader better understand the code's function
  * This is open source software. Consider the people who will read your code, and make it easier them.


Thanks,  
 

Jim Stroup  
Branch Chief, Technology Exploitation & Development (SIME)  
Mission Integration, Strategic Integration Directorate 
