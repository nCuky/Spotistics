import tkinter as tk
# import tkinter.ttk as ttk
import seaborn as sns


class GUI:
    '''
    Application GUI manager, built upon "tkinter" library.
    '''

    def __init__(self):
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

        ent_artist_name = tk.Entry(width=50,
                                   master=frm_features)
        ent_artist_name.pack(side=tk.LEFT)

        btn_audio_features = tk.Button(text="Audio Features",
                                       width=30,
                                       height=1,
                                       master=frm_features)
        btn_audio_features.pack(side=tk.TOP)

        # Frame: My top artists per month
        frm_my_artists = tk.Frame(master=self.window, relief=tk.RAISED, borderwidth=1)
        frm_my_artists.grid(row=1, column=0, padx=5, pady=5)

        lbl_year = tk.Label(text="Desired Year:",
                            master=frm_my_artists)
        lbl_year.pack(side=tk.LEFT)

        ent_year = tk.Entry(width=50,
                            master=frm_my_artists)
        ent_year.pack(side=tk.LEFT)

        btn_my_artists = tk.Button(text="My Top Artists per month",
                                   width=40,
                                   height=1,
                                   master=frm_my_artists)
        btn_my_artists.pack(side=tk.TOP)

        artist_name = ent_artist_name.get()

        self.window.mainloop()

        # # SeaBorn logic
        # df = sns.load_dataset("penguins")
        # sns.pairplot(df, hue="species")

    def close(self):
        self.window.destroy()
