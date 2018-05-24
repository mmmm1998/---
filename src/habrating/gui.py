import os
import platform
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtWidgets import QApplication, QMainWindow, QFileDialog
from PyQt5 import uic

from . import logger
from . import model
from . import db

def run_gui ():
    """
    Run habrating in GUI mode
        :return: exit code
    """
    app = QApplication (sys.argv)
    window = MainWindow ()
    window.show ()
    return app.exec_ ()
    
class MainWindow (QMainWindow):
    def __init__ (self):
        super (MainWindow, self).__init__ ()
        
        dir_path = os.path.dirname(os.path.realpath(__file__))
        uic.loadUi (dir_path+"/mainwindow.ui", self)
        
        self.tab_widget.setCurrentIndex (0)
        self.direct_dummy.hide ()
        self.url_dummy.show ()
        self.direct_tab_size = None
        self.url_tab_size = None
        
        self.setWindowTitle ("Habrating")
        self.resize (self.minimumSizeHint ())
        self.statusbar.showMessage ("Ready")
        
        self.predict_button.clicked.connect (self.on_predict_clicked)
        self.tab_widget.currentChanged.connect (self.on_tab_switched)
        
        #filename = QFileDialog.getOpenFileName (self, "Select model for prediction", "", "Model files (*.hubmodel)")
        #logger.info ("Selected model file " + filename[0])
        #self.model = model.load_model (filename[0])
        
        # List all "*.hubmodelXX" files in current directory
        arch = platform.architecture()[0].replace('bit','')
        ext_mask = ".hubmodel" + arch
        (_, _, filenames) = next (os.walk (os.getcwd ()))
        filenames = list (filter (lambda s: ext_mask in s, filenames))
        logger.info ("Found models: " + str (filenames))
        # Fill selection list with them
        self.model_selector.clear ()
        self.model_selector.addItems (filenames)
        self.model_selector.itemSelectionChanged.connect (self.on_model_selected)
        if len (filenames) == 1:
            self.model_selector.setCurrentRow (0)
        else:
            self.model = None
        
    def get_int_from_field (self, field):
        text = field.text ()
        if len (text):
            return int (text)
        return int (field.placeholderText ())
        
    def get_float_from_field (self, field):
        text = field.text ()
        if len (text):
            return float (text)
        return float (field.placeholderText ())
        
    def predict_url (self, url):
        """
        Predict rating by URL to article
            :param url: self-descriptive
            :return: estimate rating
        """
        return self.model.predict_by_urls ([url])[0]
    
    def predict_direct (self, data):
        """
        Predict rating by directly fed article
            :param data: dict in default format (with 'title', 'body', etc. fields)
            :return: estimate rating
        """
        return self.model.predict_by_posts ([data])[0]
        
    def on_model_selected (self):
        filename = self.model_selector.currentItem ().text ()
        logger.info ("Selected " + filename)
        self.model = model.load_model (filename)
        
    def on_predict_clicked (self):
        if not self.model:
            self.statusbar.showMessage ("No model selected!")
            return
        url = self.url_field.text ()
        self.statusbar.showMessage ("I'm thinking, wait a minute...")
        if self.tab_widget.currentIndex () == 0:
            try:
                logger.info (f"predicting by url {url}")
                if not url:
                    self.statusbar.showMessage ("No input")
                    return
                score = self.predict_url (url)
                self.result_field.setText (f"You will get {int (round (score))} point(s)")
                self.statusbar.showMessage ("Done!")
            except:
                self.statusbar.showMessage ("Error while predicting! (invalid URL or connection failure)")
        else:
            try:
                logger.info (f"predicting by direct feed")
                data = {}
                data['title'] = self.title_field.text ()
                # data['title length'] = len (data['title']) # Not used now
                data['body'] = self.text_field.toPlainText ()
                data['body length'] = len (data['body'])
                # Do we need to zero out these fields?
                data['views'] = 0
                data['comments'] = 0
                data['bookmarks'] = 0
                data['rating'] = 0
                data['company rating'] = self.get_float_from_field (self.comrating_edit)
                data['author rating'] = self.get_float_from_field (self.arating_edit)
                data['author karma'] = self.get_float_from_field (self.akarma_edit)
                data['author followers'] = self.get_int_from_field (self.asubs_edit)
                data['year'] = self.get_int_from_field (self.year_edit)
                logger.info (f"Input post: {data}")
                score = self.predict_direct (data)
                self.result_field.setText (f"You will get {int (round (score))} point(s)")
                self.statusbar.showMessage ("Done!")
            except ValueError:
                self.statusbar.showMessage ("Wrong input! Make sure you write numbers in additional fields")

    def change_tab_size (self, new_size):
        # Update widgets layout
        self.updateGeometry ()
        QCoreApplication.sendPostedEvents ()
        QCoreApplication.processEvents ()
        self.setMinimumSize (self.minimumSizeHint ())
        # Resize to stored size of a new tab
        self.resize (new_size or self.minimumSizeHint ())
            
    def on_tab_switched (self, idx):
        if idx == 0:
            # Store size of tab that is about to be closed
            self.direct_tab_size = self.size ()
            # Change widgets visibility and resize to previosly stored size
            self.direct_dummy.hide ()
            self.url_dummy.show ()
            self.change_tab_size (self.url_tab_size)
        else:
            # Store size of tab that is about to be closed
            self.url_tab_size = self.size ()
            # Change widgets visibility and resize to previosly stored size
            self.url_dummy.hide ()
            self.direct_dummy.show ()
            self.change_tab_size (self.direct_tab_size)
