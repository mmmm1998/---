from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow
from PyQt5 import uic

class MainWindow (QMainWindow):
    def __init__ (self):
        super (MainWindow, self).__init__ ()
        
        uic.loadUi ("mainwindow.ui", self)
        
        self.setWindowTitle ("Habrating")
        self.resize (800, 600)
        self.statusbar.showMessage ("Ready")
        
        self.predict_button.clicked.connect (self.predict)
        
    def getIntFromField (self, field):
        text = field.text ()
        if len (text):
            return int (text)
        return 0
        
    def predict (self):
        try:
            # Extract data from input fields
            title = self.title_field.text ()
            body = self.text_field.toPlainText ()
            views = self.getIntFromField (self.views_edit)
            comments = self.getIntFromField (self.comments_edit)
            bmarks = self.getIntFromField (self.bmarks_edit)
            author_rating = self.getIntFromField (self.arating_edit)
            author_karma = self.getIntFromField (self.akarma_edit)
            author_subs = self.getIntFromField (self.asubs_edit)
            
            self.statusbar.showMessage ("I'm thinking, wait a minute...")
            # TODO: insert actual model here
            # By the way, what's the accuracy of this method?
            self.result_field.setText (f"You will get {hash (title + body) % 350} point(s)")
            self.statusbar.showMessage ("Done!")
        except ValueError:
            self.statusbar.showMessage ("Wrong input! Only integers are allowed in additional fields")