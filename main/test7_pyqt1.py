'''
Created on 2014-4-19

@author: wangweisheng
'''
from PyQt5 import QtGui, QtCore

app = QtGui.QApplication([])

w = QtGui.QWidget()

def showMsg():
    QtGui.QMessageBox.information(w, u"信息", u"ok")

btn = QtGui.QPushButton(u"点我", w)
w.connect(btn, QtCore.SIGNAL("clicked()"), showMsg)

w.show()
app.exec_()