# Tasks

### DB and Logic
- [ ] Multi-Threading - when fetching large quantity of data, optimize performance so that it fetches using multi processing.
- [ ] Write logic for creating listen history DataFrame from the DB instead of the files.
- [x] Change Artist-Tracks and Artist-Albums DB tables so that they contain only links by entities' ID, without redundancy.


### Ideas for graphs
- [ ] A given artist's tracks collected by track's Key and Mode
- [ ] Top 50 artists by listening time (total time for each artist)
- [ ] Artist specialist: total listen time to an artist, as percentage of the total time of their discography's tracks.
- [ ] Forgotten tracks: listened tracks with the longest time gap between listens
- [ ] Unpopular tracks: all listens to the least-popular tracks
- [ ] Which track was listened the most times during the shortest amount of time
- [ ] Top track per month
- [ ] Top genre per section of the day (morning, noon, evening, night)


### Low priority issues regarding data standardization:
- [x] Track that was replaced with its "Remastered" version is considered a new and separate track than the original one,
 even though I want to count it as the same track. This was **solved** by getting the RelinkedTrackID for each track
 and determining exactly one "Track Known ID" according to it.
- [ ] Track that was releases as a "**Single**" is taken from its parent album, but its "**Album Name**" got the value of the "**Single**" name.
 This can **maybe** be solved by getting (alongside the Relinked Track ID) the Relinked **Album** ID, and overriding the DataFrame with it. \
 This is dangerous and I don't want to do it right now.
 Maybe build a new method specifically for replacing tracks' "Single" album ID with their "Real Album" ID.


### Done:
- [x] Column function: from all listen data, for every combination of Artist+Track+Album, find how many instances exist.
- [x] make sure the API returns ALL items and not only the first 100 - send 'offset' parameter to the API function calls
rewrite all functions so that they use this new logic.
- [x] GUI - 'get audio features' button -> show graph
- [x] Standardize the TrackKnownID for each listened track.
- [x] ~~example playlist, 746 songs: 'ProgPsy Rock and Neo Psy Rock'~~



#### OBSOLETE
* Ver.1: Always search for the "original" track ID, saved in 'linked_from' object of a Track.
See more info here: https://developer.spotify.com/documentation/general/guides/track-relinking-guide/
  In short, need to get the relinked_id for every track, and then put alongside the "track_id" already there (considered the "original_id").
* Ver.2: This is difficult, as no TrackID can be considered "the original". The relinked ID is relative to market and there is no single
market that I can depend on, which is considered "the absolute" market.
Also, it is not 100% guaranteed that there will even be a 'LinkedTrack' for a given track, even if it reasonably "should" be.
Instead, I use the combination of "Artist name + Album name + Track Title", known as "TrackCombo".
* Ver.3: Compromise and final decision:
0. *** IMPORTANT NOTICE *** The philosophical question of "What is considered a Song" is a very complicated question
which I can't and won't solve right now.
1. For every "TrackCombo", collect all distinct track_id values and put them in an Array for that TrackCombo.
2. Create a new column in the 'all_tracks' dataframe, named 'unified_track_id'
