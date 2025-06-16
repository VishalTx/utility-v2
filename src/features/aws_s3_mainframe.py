# deluxe_d3/features/aws_s3_main_frame.py

import tkinter as tk
from tkinter import ttk
#from deluxe_d3.utils.s3_file_chooser_tk import S3FileChooserTk
from DeluxeD3.DeluxeD3.src.utils.s3_file_chooser import S3FileChooser
from PIL import Image, ImageTk
import threading

class AWSS3MainFrame(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Deluxe D3 AWS S3!")
        self.geometry("800x550")
        self.configure(bg="white")

        # Header label
        header_label = ttk.Label(
            self,
            text="AWS S3 Bucket is Loading the Parquet Files!",
            font=("Comic Sans MS", 16, "bold"),
            anchor="center"
        )
        header_label.pack(pady=10)

        # Image loading (loading gif)
        try:
            img_path = "resources/Icon/loadIcon.gif"  # Adjust as needed
            image = Image.open(img_path)
            photo = ImageTk.PhotoImage(image)
            image_label = ttk.Label(self, image=photo)
            image_label.image = photo  # Keep a reference
            image_label.pack(pady=20)
        except Exception as e:
            print("Failed to load image:", e)

        # Open file chooser after loading screen
        self.after(1000, self.open_s3_chooser)

    def open_s3_chooser(self):
        try:
            chooser = S3FileChooser()
            chooser.mainloop()
        except Exception as e:
            print("Error opening S3 File Chooser:", e)

if __name__ == "__main__":
    AWSS3MainFrame().mainloop()
