# _Spotistics_: Analysis & Statistics of your Spotify Listening History 

[Spotify](spotify.com) is a very popular music streaming service, with over 400M monthly active users and 80M tracks.
I (Nadav) have been listening to music in Spotify since 2017. Throughout the years, I listened to more than 60,000
songs.
I was curious to learn about my musical journey "from above", and try to define my musical identity, based on my
listening history.

This project uses a dataset with all the tracks I've listened to ever since I created my Spotify account in
January 2017 up until September 2022. Using the Spotify API, the app enhances the dataset with additional metadata and
musical data, and generates plots about my listening habits and musical preferences, such as top artists, preferred
genres per time of day,
most common musical mode and key, and more.

### Project's Goals


### Prerequisites

1. **[Request to download your personal Data from Spotify](mailto:privacy@spotify.com)** - specifically, request to get
   the **extended streaming history and other technical information for the life of your account**. 
   
   This can take up to 30 days (in practice it's usually less than that), so please be patient. When you receive a response, download your data and put it 
   in a folder the app can access, especially the `endsong.json` files that contain your listening history.

   Sadly, it is **impossible** to instantly fetch the complete listening history of one's Spotify account **on-demand**.
   The Spotify API only allows to fetch the last 50 recently played tracks, and no more. This is a hard limitation 
   of the API that unfortunately cannot be circumvented. 
   For this reason, the app needs to use a pre-downloaded dataset. Every Spotify user can go to their account's
   [Spotify Privacy Dashboard](https://www.spotify.com/us/account/privacy/#:~:text=Download%20your%20data), and request
   their data in one of two ways:

    - Clicking the **Request** button at the bottom of that page, using the automated "Download your Data" tool. This
      method yields the streaming history **for the past year** and no more, and at the moment **is not supported by 
      this project**;
    - **[Manually contacting](mailto:privacy@spotify.com)** Spotify and requesting the extended streaming history for 
      the life of your account. 
      This method **is preferred** and is used by this project, but its major drawback is that
      it can take up to 30 days to get your data (personally, I received my data after 10 days).

2. 

## Authors

🧔🏻 **Nadav Curiel**

- Github: [@nCuky](https://github.com/nCuky)
- LinkedIn: [Nadav Curiel](https://linkedin.com/in/nadav-curiel)

🧔🏻 **Eshom**

- Github: [@eshom](https://github.com/eshom)

