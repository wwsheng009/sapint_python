'''
Created on 2014-4-19

@author: wangweisheng
'''
import os
import gc
import psutil

from PyQt5.QtCore import (QFile, QFileInfo, QPoint, QRect, QSettings, QSize,
                          Qt, QTextStream)
from PyQt5.QtGui import QIcon, QKeySequence
from PyQt5.QtWidgets import (QAction, QApplication, QFileDialog, QMainWindow,
                             QMessageBox, QTextEdit, QTableWidget, QTableWidgetItem, QComboBox, QHBoxLayout, QWidget)

from PyQt5 import uic


import json
import sapint

from sapint import SAPException
from sapnwrfc import RFCException
from sapnwrfc import RFCCommunicationError
from sapnwrfc import RFCServerError
from sapnwrfc import RFCFunctionCallError

from sapint.tableutil.ReadTable import CReadTable
logger = sapint.getCommentLogger(__name__)
proc = psutil.Process(os.getpid())

class TableRearder(QMainWindow):

	bool_table_changed = False
	current_tableName = ''
	current_destName = ''

	table_settings = {}

	rowcount = 0
	delimeter = ''

	current_table_ctrl = object

	def __init__(self):
		super(TableRearder, self).__init__()

		#self.ui = uic.loadUi("test-ok.ui", self)
		#load the ui file 
		__uifile = os.path.join(os.path.dirname(__file__), 'saptablereader.ui')
		self.ui = uic.loadUi(__uifile, self)


		#self.ui.txtTableName.setStyleSheet("background-color: rgb(0, 40, 100);")

		syslist = sapint.get_sap_client_list()
		for k, v in syslist.items():
			self.ui.cbxSap.addItem(k, v)

		self.ui.spinCount.setValue(10)


		self.ui.tblFields.setStyleSheet("selection-background-color:lightblue;")

		#button clicked
		self.ui.btnRun.clicked.connect(self.run)
		self.ui.btnReadFields.clicked.connect(self.readTableFields)
		self.ui.btnAddField.clicked.connect(self.actionAddField)
		self.ui.btnDeleField.clicked.connect(self.actionDelField)
		self.ui.btnSwitchSelectField.clicked.connect(self.switchFieldsSelected)
		self.ui.btnSwitchSelectCon.clicked.connect(self.switchCondsSelected)
		self.ui.btnDelCon.clicked.connect(self.actionDelCon)
		self.ui.btnAddCon.clicked.connect(self.actionAddCon)
		self.ui.btnSwitchHeader.clicked.connect(self.switchHeader)

		#other events
		self.ui.tblFields.itemClicked.connect(self.tableClicked)
		self.ui.txtTableName.textChanged.connect(self.tableNameChanged)

		#add the close action to tab
		close_action = QAction("&Close",self.ui.tabTables)
		close_action.triggered.connect(self.actionCloseTab)

		closeall_action  = QAction("Close &All",self.ui.tabTables)
		closeall_action.triggered.connect(self.actionCloseAllTab)

		self.ui.tabTables.addAction(close_action)
		self.ui.tabTables.addAction(closeall_action)
		self.ui.tabTables.setContextMenuPolicy(Qt.ActionsContextMenu)


	def readTableFields(self):
		self.getUIValues()
		from rfcfns import rfc_tableFields
		tname = self.current_tableName
		fields = rfc_tableFields.tablefieldsinfo(self.current_destName, self.current_tableName)
		self.save_table_settings(tname)
		if tname not in self.table_settings.keys():
			self.table_settings[tname] = {}

		self.table_settings[tname]['fields'] = fields
		self.restore_table_settings(tname)        

		pass
	def getCurrentTableWidget(self):
		'''
		获取当前选中的TableWidget控件
		返回当然选中的TableWidget控件的文本
		'''
		count = self.ui.tabTables.count()
		tabText = ''
		if count > 0:
			#get the widget in the tabs
			tab = self.ui.tabTables.currentWidget()
			if tab !=None:
				tabText = self.ui.tabTables.tabText(self.ui.tabTables.currentIndex())

				print( tabText ,'tab destroyed')
				#get the tableWidget in the tab
				t = tab.layout().itemAt(0).widget()
				self.current_table_ctrl = t
				print('rowcount:',t.rowCount())
		else:
			print("no tab found")
		return tabText

	def switchHeader(self):
		'''
		切换显示表格控件的抬头文本。在字段名与字段文本之间切换
		'''
		headerlabels = []
		tabname = self.getCurrentTableWidget()
#         print('switch header',self.current_tableName)
		if tabname in self.table_settings.keys():
			if 'fields' in self.table_settings[tabname].keys():
				headers = self.table_settings[tabname]['fields']

				if 'HEADERTYPE' not in self.table_settings[tabname].keys():
					return
				ftype = self.table_settings[tabname]['HEADERTYPE']
				if ftype == 'FIELDTEXT':
					headerlabels = [x['FIELDNAME'] for x in headers]
					self.current_table_ctrl.setHorizontalHeaderLabels(headerlabels)
					self.table_settings[tabname]['HEADERTYPE'] = 'FIELDNAME'
				elif ftype == 'FIELDNAME':
					headerlabels = [x['FIELDTEXT'] for x in headers]
					self.current_table_ctrl.setHorizontalHeaderLabels(headerlabels)
					self.table_settings[tabname]['HEADERTYPE'] = 'FIELDTEXT'



	def tableNameChanged(self, tname):
		self.bool_table_changed = True
		self.current_tableName = tname
		#print('tname', tname)
#         self.save_table_settings(tname)
		self.restore_table_settings(tname)

	def save_table_settings(self, tname):
		'''
		根据表名，保存当前界面上的字段与条件设置
		'''
		if tname not in self.table_settings.keys():
			self.table_settings[tname] = {}
		if 'fields' not in self.table_settings[tname].keys():
			self.table_settings[tname]['fields'] = {}
			self.table_settings[tname]['fields'] = self.get_fields()

		if 'conds' not in self.table_settings[tname].keys():
			self.table_settings[tname]['conds'] = {}
			self.table_settings[tname]['conds'] = self.get_Conditions()
#         
#         self.ui.tblFields.clearContents()
#         self.ui.tblConditions.clearContents()

	def restore_table_settings(self, tname):
		'''从缓存中读取字段列表，条件，并恢复到界面控件'''
		self.ui.tblConditions.clearContents()
		self.ui.tblFields.clearContents()
		self.ui.tblFields.setRowCount(0)
		self.ui.tblConditions.setRowCount(0)

		fields_tmp = []
		conds_tmp = []
		if tname not in self.table_settings.keys():
			self.ui.tblConditions.clearContents()
			self.ui.tblFields.clearContents()
			return

		if self.table_settings[tname] != None and self.table_settings[tname] != '':
			if(self.table_settings[tname] != None):
				if 'fields' in self.table_settings[tname].keys():
					fields_tmp = self.table_settings[tname]['fields']
			if(self.table_settings[tname] != None):
				if 'conds' in self.table_settings[tname].keys():
					conds_tmp = self.table_settings[tname]['conds']

		self.ui.tblFields.setRowCount(len(fields_tmp))
		for h in range(len(fields_tmp)):
#             print(header[h])
			self.MyCombo = QComboBox()
			self.MyCombo.addItem("O", True) 
			self.MyCombo.addItem("X", False)
			if 'CHCKED' in fields_tmp[h].keys():
				self.MyCombo.currentIndex = fields_tmp[h]['CHCKED']
			self.ui.tblFields.setCellWidget(h, 0, self.MyCombo)
			self.ui.tblFields.setItem(h, 1, QTableWidgetItem(fields_tmp[h]['FIELDNAME']));
			self.ui.tblFields.setItem(h, 2, QTableWidgetItem(fields_tmp[h]['FIELDTEXT']));


		self.ui.tblConditions.setRowCount(len(conds_tmp))
		for h in range(len(conds_tmp)):
#             print(header[h])
			self.MyCombo = QComboBox()
			self.MyCombo.addItem("O", True) 
			self.MyCombo.addItem("X", False)
			if 'CHCKED' in conds_tmp[h].keys():
				self.MyCombo.currentIndex = conds_tmp[h]['CHCKED']  
			self.ui.tblConditions.setCellWidget(h, 0, self.MyCombo)
			self.ui.tblConditions.setItem(h, 1, QTableWidgetItem(conds_tmp[h]['COND']));

		self.ui.tblFields.resizeColumnsToContents()
		self.ui.tblConditions.resizeColumnsToContents()
	def test(self):
		print(self.get_Conditions())
		print(self.get_fields())
	def actionAddField(self):
		idx = self.ui.tblFields.rowCount()
		self.ui.tblFields.insertRow(idx)
		self.MyCombo = QComboBox()
		self.MyCombo.addItem("O", True) 
		self.MyCombo.addItem("X", False)

		self.ui.tblFields.setCellWidget(idx, 0, self.MyCombo)

	def switchCondsSelected(self):
		model = self.ui.tblConditions.selectionModel()
		if model.hasSelection():
			for idx in model.selectedRows():
				cb = self.ui.tblConditions.cellWidget(idx.row(),0)
				if cb.currentIndex() == 0:
					cb.setCurrentIndex(1)
				else:
					cb.setCurrentIndex(0)        
	def switchFieldsSelected(self):
		""""""
		model = self.ui.tblFields.selectionModel()
		if model.hasSelection():
			for idx in model.selectedRows():
				cb = self.ui.tblFields.cellWidget(idx.row(),0)
				if cb.currentIndex() == 0:
					cb.setCurrentIndex(1)
				else:
					cb.setCurrentIndex(0)
				#v = self.ui.tblFields.item(idx.row(),1)

				#print(v.text())
				#print(idx.row(),v == None)

	def actionDelField(self):
		select = self.ui.tblFields.selectionModel()
		if select.hasSelection():
			for index in select.selectedIndexes():
				self.ui.tblFields.removeRow (index.row())
	def actionDelCon(self):
		select = self.ui.tblConditions.selectionModel()
		if select.hasSelection():
			for index in select.selectedIndexes():
				self.ui.tblConditions.removeRow (index.row())

	def actionAddCon(self):
		idx = self.ui.tblConditions.rowCount()
		self.ui.tblConditions.insertRow(idx)
		self.MyCombo = QComboBox()
		self.MyCombo.addItem("O", True) 
		self.MyCombo.addItem("X", False)
#             self.MyCombo.addItem("鈭�)
		print('idx', idx)
		self.ui.tblConditions.setCellWidget(idx, 0, self.MyCombo)



	def tableClicked(self, item=None):
		print('test')
	def get_Conditions(self):
		rows = self.ui.tblConditions.rowCount()
		conds = []
		for row in range(rows):
			cond = {}
			if(self.ui.tblConditions.cellWidget(row, 0) != None):
				chk = self.ui.tblConditions.cellWidget(row, 0).currentData()
				feld = self.ui.tblConditions.item(row, 1)
				cond['CHECKED'] = chk
				if(feld != None):
					cond['COND'] = feld.text()
					conds.append(cond)


		return conds
	def get_fields(self):
		rows = self.ui.tblFields.rowCount()

		fields = []

		for row in range(rows):
			field = {}
			if(self.ui.tblFields.cellWidget(row, 0) != None):

				chk = self.ui.tblFields.cellWidget(row, 0).currentData()
				field['CHECKED'] = chk
				feld = self.ui.tblFields.item(row, 1)
				feldt = self.ui.tblFields.item(row, 2)
				if(feld != None):
					field['FIELDNAME'] = feld.text()
					if feldt != None:
						field['FIELDTEXT'] = feldt.text()
					fields.append(field)

		return fields
	def run(self):
		#try:
		self.excute()
		#except Exception as ex:           
			#QMessageBox.critical(None, "Systray",
							#"Errors:{0}".format(ex))
		#exit


	def getUIValues(self):
		self.current_destName = self.ui.cbxSap.currentText()


		self.current_tableName = self.ui.txtTableName.text()
		self.delimeter = self.ui.txtDelimeter.text()
		self.rowcount = self.ui.spinCount.value()

		print("sys:{0},tablename:{1},delimeter:{2},rowcount:{3}".format(
		        self.current_destName, self.current_tableName, self.delimeter, self.rowcount))

		if(self.current_tableName == ''):
			QMessageBox.critical(None, "Systray",
			                     "表名是必须的")
			return False
		return True

	def excute(self):
		
		 
		#from pympler import tracker

		#memory check
		#memory_tracker = tracker.SummaryTracker()

#         self.ui.table.clear()
		self.getUIValues()
		tname = self.current_tableName
		if tname not in self.table_settings.keys():
			self.table_settings[tname] = {}       		

		table = CReadTable(self.current_destName)
		table.TableName = tname
		table.RowCount = self.rowcount
		table.Delimiter = self.delimeter
		fields = self.get_fields()
		conds = self.get_Conditions()

		for c in conds:
			table.AddCriteria(c['COND'])
		#print('fields', fields)
		for f in fields:
			if f['CHECKED'] == True:
				table.AddField(f['FIELDNAME'])
		table.Run()


		result = table.GetResult()
		#logger.info(json.dumps(result))
		headers = table.GetFields()
		#logger.info(json.dumps(headers))

		print('the table len is ',len(result))
		#print('self.bool_table_changed', self.bool_table_changed)

		if(self.bool_table_changed == True):
			if self.get_fields() == []:
				self.table_settings[tname]['fields'] = headers
				self.table_settings[tname]['conds'] = []
				self.restore_table_settings(tname)
				self.bool_table_changed = False
		#else:
			#self.save_table_settings(tname)
			#if tname not in self.table_settings.keys():
				#self.table_settings[tname] = {}

			#if self.get_fields() == []:
				#self.table_settings[tname]['fields'] = headers
				#self.restore_table_settings(tname)


		#add a new temp tablewidget into the tab
		ctrlTableTmp = QTableWidget()
		ctrlTableTmp.setColumnCount(len(headers))
		ctrlTableTmp.setRowCount(len(result))
#         headerlabels = [x['FIELDNAME'] for x in header]
		headerlabels = [x['FIELDTEXT'] for x in headers]
		self.table_settings[tname]['HEADERTYPE'] = 'FIELDNAME'
		ctrlTableTmp.setHorizontalHeaderLabels(headerlabels)

		for x  in range(len(result)):
			for y in range(len(result[x])):
				if result[x][y]!=None and result[x][y]!='':
				#print(result[x][y])
					ctrlTableTmp.setItem(x, y, QTableWidgetItem(str(result[x][y])))
			result[x] = {}

		ctrlTableTmp.resizeColumnsToContents()
		#self.ui.tblFields.resizeColumnsToContents()

		self.current_table_ctrl = ctrlTableTmp

		layout = QHBoxLayout() 
		layout.addWidget(ctrlTableTmp)  
		widget = QWidget()
		widget.setLayout(layout)

		#self.ui.tabTables.tabsClosable = True
		self.ui.tabTables.addTab(widget, tname)
		self.ui.tabTables.setCurrentWidget(widget)

		#memory check 
		#memory_tracker.print_diff()
	def actionCloseAllTab(self):
		'''try to close all the tab'''
		count = self.ui.tabTables.count()
		#self.ui.tabTables.clear()
		print('tabcount',count)
		if count > 0:
			for i in range(count-1,-1,-1):

				#print('widget item {0} to be destroyed'.format(i))
				#get the widget in the tabs
				tab = self.ui.tabTables.widget(i)
				#get the tableWidget in the tab
				t = tab.layout().itemAt(0).widget()
				t.destroy()
				self.ui.tabTables.removeTab(i)
				gc.collect()
		else:   
			print("no tab to close")

		self.ui.tabTables.repaint()



	def actionCloseTab(self):
		'''when the tab closed will trigged this action'''
		count = self.ui.tabTables.count()
		if count > 0:
			#get the widget in the tabs
			tab = self.ui.tabTables.currentWidget()
			if tab !=None:
				#print('tab destroyed')
				#get the tableWidget in the tab
				t = tab.layout().itemAt(0).widget()
				#print('rowcount:',t.rowCount())
				t.destroy()
				self.ui.tabTables.removeTab(self.ui.tabTables.currentIndex())
				gc.collect()
		else:
			print("no tab to close")
		pass

if __name__ == "__main__":
	import sys
	app = QApplication(sys.argv)
	t = TableRearder()
	t.show()
	sys.exit(app.exec())