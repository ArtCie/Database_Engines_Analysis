from tkinter import *
from tkinter import ttk, messagebox
import time
import random

from databases.database_handler import DatabaseHandler


class InterfaceMain:
    def __init__(self, database_handler: DatabaseHandler):
        self._database_handler = database_handler
        self.root = Tk()
        self.pixel = PhotoImage(width=1, height=1)

    def draw(self):
        self._init_root()
        self._draw_title("Comparison of PostgreSQL, MongoDB and DynamoDB Engines")
        self._draw_database(title_="Database", x=60, y=50)
        self.root.mainloop()

    def _init_root(self):
        self.root.geometry("500x650")
        self.root.title("Database Engines Analysis")

    def _draw_title(self, title_, padx=5, pady=20):
        title = StringVar()
        label = Label(self.root, textvariable=title)
        title.set(title_)
        label.pack(padx=padx, pady=pady)

    def _draw_database(self, title_, x, y):
        self._draw_frame(title_, x=x, y=y)
        self._draw_button("Clear database", x=x + 35, y=y + 50, handler=self._database_handler.clear_databases)
        self._draw_button("Populate database", x=x + 35, y=y + 130, handler=self._database_handler.populate_databases)
        self._draw_button("Modify database", x=x + 35, y=y + 210, handler=self._database_handler.update_databases)
        self._draw_button("Insert to database", x=x + 35, y=y + 290)
        self._draw_button("Generate statistics", x=x + 35, y=y + 370)
        self._where_expression = self._create_input("Where expression", x=x + 35, y=y + 450)

    def _draw_frame(self, title_, x, y):
        function_frame = LabelFrame(self.root, text=title_, width=380, height=560)
        function_frame.place(x=x, y=y)

    def _draw_button(self, title_, x, y, handler=print):
        search_button = Button(self.root, text=title_, image=self.pixel, width=300, height=60,
                               command=handler, compound="center", state=NORMAL)
        search_button.place(x=x, y=y)

    def _create_input(self, text, x, y):
        function_frame = LabelFrame(self.root, text=text, width=310, height=60)
        function_frame.place(x=x, y=y)
        entry = Entry(self.root, width=28, textvariable=StringVar())
        entry.place(x=x + 15, y=y + 20)
        return entry

