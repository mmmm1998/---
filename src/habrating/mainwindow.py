from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow
from PyQt5 import uic

class MainWindow (QMainWindow):
    def __init__ (self):
        super (MainWindow, self).__init__ ()
        
        uic.loadUi ("mainwindow.ui", self)
        
        self.setWindowTitle ("Habrating")
        self.resize (800, 600)
        self.statusbar = self.statusBar ()
        self.statusbar.showMessage ("Ready")