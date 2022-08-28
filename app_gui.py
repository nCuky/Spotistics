import tkinter as tk
import logic as lg


class AppGUI:
    '''
    Application GUI manager, built upon "tkinter" library.
    '''

    def __init__(self):
        self.events = []

        self.window = tk.Tk()
        self.window.title("Cuky's Musical Analytics")

        self.window.rowconfigure(index=0, minsize=100, weight=1)
        self.window.columnconfigure(index=0, minsize=100, weight=1)
        self.window.rowconfigure(index=1, minsize=100, weight=1)

        # Frame: Audio Features
        frm_features = tk.Frame(master=self.window, relief=tk.RAISED, borderwidth=1)
        frm_features.grid(row=0, column=0, padx=5, pady=5)
        # frm_features.pack(fill=tk.BOTH, side=tk.TOP, expand=True)

        lbl_artist_name = tk.Label(text="Artist Name:",
                                   master=frm_features)
        lbl_artist_name.pack(side=tk.LEFT)

        self.ent_artist_name = tk.Entry(width=50,
                                        master=frm_features)
        self.ent_artist_name.pack(side=tk.LEFT)

        self.btn_audio_features = tk.Button(text="Audio Features",
                                            width=30,
                                            height=1,
                                            master=frm_features,
                                            command=self.on_click_audio_features)

        self.btn_audio_features.pack(side=tk.TOP)

        # Frame: My top artists per month
        frm_my_artists = tk.Frame(master=self.window, relief=tk.RAISED, borderwidth=1)
        frm_my_artists.grid(row=1, column=0, padx=5, pady=5)

        lbl_year = tk.Label(text="Desired Year:",
                            master=frm_my_artists)
        lbl_year.pack(side=tk.LEFT)

        self.ent_year = tk.Entry(width=50,
                                 master=frm_my_artists)
        self.ent_year.pack(side=tk.LEFT)

        self.btn_my_artists = tk.Button(text="My Top Artists per month",
                                        width=40,
                                        height=1,
                                        master=frm_my_artists,
                                        command=self.on_click_my_artists)
        self.btn_my_artists.pack(side=tk.TOP)

        self.window.mainloop()

        # # SeaBorn logic
        # df = sns.load_dataset("penguins")
        # sns.pairplot(df, hue="species")

    def close(self):
        self.window.destroy()


    def on_click_audio_features(self):
        artist_name = self.ent_artist_name.get()
        data = lg.get_artist_audio_features_data(artist_name)
        # sns.barplot(data=data, x="features", y="mode")

    def on_click_my_artists(self):
        artist_name = self.ent_artist_name.get()
