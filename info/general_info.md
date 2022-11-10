# _Spotistics_: General Information #

### Track's Audio Features
Spotify keeps a number of musical and sonic attributes about its tracks. These attributes are
called
**[Audio Features](https://developer.spotify.com/documentation/web-api/reference/#/operations/get-audio-features)**,
and they give us interesting musical analysis of a given track:

- **Acousticness**: A confidence measure between 0.0-1.0 of how much the track is **acoustic**
  (as opposed to electronic, distorted or synthesized).
  Higher value means more confidence of accousticness.


- **Danceability**: A measure between 0.0-1.0 of how much a track is **suitable for dancing**, based on a combination of
  musical
  elements including tempo, rhythm stability, beat strength, and overall regularity. Higher value is more danceable.


- **Energy**: A perceptual measure between 0.0-1.0 of perceived **intensity and activity**.
  Typically, energetic tracks feel fast, loud, and noisy. This feature measures the dynamic range,
  perceived loudness, timbre, onset rate, and general entropy. Higher value means more energetic.


- **Instrumentalness**: A prediction between 0.0-1.0 of whether a track contains **no vocals**.
  Higher value (0.5 and above) means the greater likelihood the track contains no vocal content.


- **Liveness**: Detects the presence of an **audience** in the recording. Higher value (0.8 and above) represents an
  increased
  probability that the track was performed live.


- **Loudness**: The overall (average) **loudness in decibels (dB)** of a track.
  Loudness is the primary psychological correlate of physical strength (amplitude).
  Value typically range between -60 and 0 db.


- **Speechiness**: Detects the presence of **spoken words** in a track.
  Value above 0.66 describe tracks that are probably made entirely of spoken words.
  Value between 0.33 and 0.66 describe tracks that may contain both music and speech.
  Value below 0.33 most likely represent music and other non-speech-like tracks.


- **Valence**: A measure between 0.0-1.0 of the **musical positiveness** conveyed by a track. Higher value means the
  track sound more positive (e.g. happy, cheerful, euphoric).


- **Tempo**: The overall estimated **tempo ("speed")** of a track, in beats per minute (BPM).
  For example, _"Stayin' Alive"_ by the Bee Gees has a tempo of approximately 103 BPM. 
