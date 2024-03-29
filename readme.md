# _Spotistics_: Analysis & Statistics of your Spotify Listening History 
[Spotify](https://www.spotify.com) is a very popular music streaming service, with over 400M monthly active users and 80M tracks.
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
Define and summarize one's musical taste; Identify patterns, trends and other interesting properties of one's listen 
history, and display them in a coherent and simple manner.  

### Prerequisites
1. **[Request to download your personal Data from Spotify](https://www.spotify.com/us/account/privacy/#:~:text=Select%20Account%20data-,Extended%20streaming%20history,-Extended%20streaming%20history)** - specifically, request to get
   the **extended streaming history for the life of your account**. 
   
   This can take up to 30 days (in practice it's usually less than that), so please be patient. 
   
   **Explanation**: 
   
   Sadly, it is **impossible** to instantly fetch the _complete_ listening history of one's Spotify account **on-demand**.
   The Spotify API only allows to fetch the last 50 recently played tracks, and no more. This is a hard limitation 
   of the API that unfortunately cannot be circumvented. 
   For this reason, the app needs to use a pre-downloaded dataset. Every Spotify user can go to their account's
   [Privacy Dashboard](https://www.spotify.com/us/account/privacy/#:~:text=Select%20Account%20data-,Extended%20streaming%20history,-Extended%20streaming%20history),
   and request their data:
   - Scroll down to "**Download your data**"
   - Check "**Select Extended streaming history**"
   - Click the **Request data** button at the bottom of the page
   - **Note:** The "Account data" method yields the listen history only for the past year, and it's currently **not supported by this app**. Be sure to 
     download the Extended streaming history.
     It can take up to 30 days to get your data. Personally, I received my data after 10 days.

2. Clone this project into a folder of your choice.

3. Extract the downloaded archive file containing your personal Spotify data, and put its contents in a folder the 
   _Spotistics_ app can access, especially the `endsong.json` files that contain your listening history. The default 
   location for that is in the project folder, in `data\personal_data\raw_json`.
    - If your files are in a different folder, please edit file `config.py` and change the value of
      `DEFAULT_JSON_FILE_PATH` to the folder of your choice.
    - If your `endsong.json` files are renamed, please change the value of `DEFAULT_JSON_FILE_PREFIX` 
      (also in `config.py`) to the correct prefix.

4. Edit `config.py` and change the value of `LISTEN_HISTORY_SRC` to '**json**'. 

### Instructions
Run the `main.py` module. If everything went smoothly, some plots should be displayed. 
Then, please edit file `config.py` again, and change `LISTEN_HISTORY_SRC` back to '**db**'.

### Authors
🧔🏻 **Nadav Curiel**
- Github: [@nCuky](https://github.com/nCuky)
- LinkedIn: [Nadav Curiel](https://linkedin.com/in/nadav-curiel)

🧔🏻 **Eshom**
- Github: [@eshom](https://github.com/eshom)
