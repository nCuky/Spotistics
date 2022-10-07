# "Spotistics": Spotify Listening History Analysis & Statistics

[Spotify](spotify.com) is a very popular music streaming service, with over 400M monthly active users and 80M tracks.
I have been listening to music in Spotify since 2017. Throughout the years, I listened to more than 60,000 songs.
I was curious to learn about my musical journey "from above", and try to define my musical identity, based on my
listening history.

This project uses a dataset with all the tracks I've listened to ever since I created my Spotify account in
January 2017 up until September 2022. Using the Spotify API, the app enhances the dataset with additional metadata and
musical data, and generates plots about my listening habits and musical preferences, such as top artists, preferred
genres per time of day,
most common musical mode and key, and more.

### Prerequisites

1. **[Download your complete Spotify Personal Data](privacy@spotify.com)** (extended streaming history and other
   technical information),
   and put it in a folder the app can access, specifically the `endsong.json` files that contain your listening history.

   Sadly, it is **impossible** to fetch the complete listening history of one's Spotify account instantly **on-demand**.
   The Spotify API allows for only the last 50 recently played tracks to be fetched, and no more.
   For this reason, the app needs to use a pre-downloaded dataset. Every Spotify user can go to their account's
   [Spotify Privacy Dashboard](https://www.spotify.com/us/account/privacy/#:~:text=Download%20your%20data), and request
   their data in one of two ways:

    - Clicking the **Request** button at the bottom of that page, using the automated "Download your Data" tool. This
      method
      gives the streaming history **for the past year** and no more, and at the moment **is not supported** by this
      project;
    - **Manually contacting** Spotify and requesting the extended streaming history for the life of
      your account. This method **is preferred** and used by this project, but its major drawback is that
      it can take up to 30 days to get your data (personally, I received my data after 10 days).

2. 

### Track's Audio Features

Spotify keeps some musical and sonic attributes about its tracks. These attributes are
called
**[Audio Features](https://developer.spotify.com/documentation/web-api/reference/#/operations/get-audio-features)**,
and they give us an interesting musical analysis of a given track:

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


## Author
🧔🏻 **Nadav Curiel**
- Github: [@nCuky](https://github.com/nCuky)
- 