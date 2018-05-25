import progressbar

def get_bar(maxval):
    """
    Return customized progress bar
        :param maxval: maxval for bar
    """
    widgets=[
    '[', progressbar.Timer(), '] ',
    '[', progressbar.SimpleProgress(), '] ',
    progressbar.Bar(left='[', right=']'),
    ' [', progressbar.ETA(format="ETA: %S"), '] ',
    ]
    return progressbar.ProgressBar(maxval=maxval, widgets=widgets)
