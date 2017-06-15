# -*- coding: utf-8 -*-

import os
import re
import csv
import time
import numpy as np
from collections import Counter

class TrafficLogProcessing:
    def __init__(self, FileStruct):
    	self.fieldName = FileStruct
        
    ##### unzipFile(FolderPath, FolderList)
    # unzip all traffic log files under folder in FolderList
    #
    # @FolderPath: (str) the tops path of the folder in FolderList
    # @FolderList: (list) the element is target folder name
    #
    ######################################################
    def unzipFile(self, FolderPath, FolderList):
        print "unzip traffic Log File ..."

        for folder in FolderList:
            counter = 0
            print "======================================================================="
            print "Target : "+folder
            print "======================================================================="
            path = FolderPath+"/"+folder
            for tops, dirs, files in os.walk(path):
                for file in files:
                    counter += 1
                    target_gz_file = os.path.join(tops,file)
                    os.system("gzip -d "+target_gz_file)
                    if counter % 10 == 0:
                        print "progress rate: "+str(counter)+" / "+ str(len(files))
            print "progress rate: "+str(counter)+" / "+ str(len(files))
    
    ##### parseTrafficLog2Dict(FolderPath, SelectColumns, Conditions, FilterType, 
    #                         SaveUnit, SavePath)
    # save selected columns data of matching Conditions into SavePath, 
    # FilterType can give value 'non-repeat' to get non-repeat data.
    #
    # @FolderPath: (str) the tops path of the folder in FolderList
    # @SelectColumns: (list) select column will be pick and store 
    # @Conditions: (dict) filter condition
    # @FilterType: (str) have two type: "non-repeat" and "repeat", "non-repeat" will filter data not repeating
    # @SaveUnit: (int) the number of processing files are reached SaveUnit will save file 
    # @SavePath: (str) save data path
    #
    ######################################################
    def parseTrafficLog2Dict(self, FolderPath, SelectColumns, Conditions, FilterType, SaveUnit, SavePath):
        tStart = time.time()
        # initialize dict of selected columns
        dataDict = dict()
        for column in SelectColumns:
            dataDict[column] = []

        counter = 0
        total_file_count = 0

        print "parse traffic Log File ..."
        print "======================================================================="
        print "Target : "+FolderPath
        print "======================================================================="
        # parse all log file under FolderPath
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                counter += 1
                FilePath = os.path.join(tops,file)
                
                with open(FilePath, "r") as sourceFile:
                    for line in sourceFile:
                        #Get data from string
                        line=line.replace("\n","")
                        data=re.split(r"\t",line)

                        #Convert data to dictionary type
                        row=dict()
                        for i in xrange(len(self.fieldName)):
                            row[self.fieldName[i]]=data[i]
                    
                        # check conditions
                        match = 0
                        for key in Conditions.keys():
                            if row.get(key) == Conditions.get(key):
                                match += 1
                    
                        # pick matching data
                        if match == len(Conditions.keys()):
                            for column in SelectColumns:
                                dataDict.get(column).append(row.get(column))

                # filter repeat data
                if FilterType == 'non-repeat':
                    for key in dataDict.keys():
                        dataDict.update({key:list(set(dataDict.get(key)))})
                
                if counter == SaveUnit:
                    print "progress rate: "+str(total_file_count)+" / "+ str(len(files))
                    total_file_count += counter
                    for key in dataDict.keys():
                        # get the key's content and set save filename
                        column_content = dataDict.get(key)
                        FileName = key+"_"+str(len(column_content))+"_"+str(total_file_count)+".txt"

                        # restruct data to SaveFormat and save data
                        column_content = self.restructureList2SaveFormat(column_content)
                        self.saveData(SavePath, FileName, column_content)

                        # clear dataDict column list
                        dataDict.update({key:[]})
                    counter = 0

            # remain part( < SaveUnit)
            total_file_count += counter
            print "progress rate: "+str(total_file_count)+" / "+ str(len(files))
            # get the key's content and set save filename
            for key in dataDict.keys():
                # get the key's content and set save filename
                column_content = dataDict.get(key)
                FileName = key+"_"+str(len(column_content))+"_"+str(total_file_count)+".txt"
                
                # restruct data to SaveFormat and save data
                column_content = self.restructureList2SaveFormat(column_content)
                self.saveData(SavePath, FileName, column_content)
                # clear dataDict column list
                dataDict.update({key:[]})

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)
    
    ##### restructureList2SaveFormat(source)
    # restructure List type data to Str type with line feed format
    #
    # @source: (list) source data
    #
    # return: 
    # @content: (str) restructured data for saving file
    #
    ######################################################
    def restructureList2SaveFormat(self, source):
        content = ""
        for row in source:
            content += row+"\r\n"
        return content
    
    ##### saveData(SavePath, FileName, Content)
    # write Content data in file
    #
    # @SavePath: (str) the path is for saving files
    # @FileName: (str) saving file name
    # @Content: (str) restructure data that can be write directly in file
    #
    ######################################################
    def saveData(self, SavePath, FileName, Content):
        with open(os.path.join(SavePath,FileName), "w") as fp:
            fp.write(Content)
    
    ##### loadData(FilePath, FileName)
    # load Content data from file, ex. White list etc.
    #
    # @FilePath: (str) the target path
    # @FileName: (str) target file name
    #
    ######################################################
    def loadData(self, FilePath, FileName):
        DataList = []
        with open(os.path.join(FilePath,FileName), "r") as fp:
            for item in fp:
                item = item.replace("\r\n","")
                DataList.append(item)
        return DataList

    ##### mergeData(FolderPath, SavePath, FileName)
    # merge data of one day into one file. And data is non-repeat
    #
    # @FolderPath: (str) the target folder path
    # @SavePath: (str) the path is for saving file
    # @FileName: (str) name of saving file
    #
    ######################################################
    def mergeData(self, FolderPath, SavePath, FileName):
        tStart = time.time()
        print "merge data ..."
        print "======================================================================="
        print "Target : "+FolderPath.split("/")[-1]
        print "======================================================================="
        all_data = list()
        counter = 0
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                counter += 1
                with open(os.path.join(tops,file), "r") as fp:
                    for line in fp:
                        #Get data from string
                        line=line.replace("\n","")
                        line=line.replace("\r","")
                        all_data.append(line)
                if counter % 100 == 0:
                    print "progress rate: "+str(counter)+" / "+ str(len(files))
                    all_data = list(set(all_data))

        print "progress rate: "+str(counter)+" / "+ str(len(files))
        all_data = list(set(all_data))
        print "num of all data : ",len(all_data)
        
        print "saving data ..."
        all_data = self.restructureList2SaveFormat(all_data)
        self.saveData(SavePath, FileName, all_data)

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)
    
    ##### selectAppearCrossDayData(FolderPath, SavePath, FileName)
    # select the data item which is appeared in all 1 day data.
    #
    # @FolderPath: (str) the target folder path
    # @SavePath: (str) the path is for saving file
    # @FileName: (str) name of saving file
    #
    ######################################################
    def selectAppearCrossDayData(self, FolderPath, SavePath, FileName):
        tStart = time.time()
        print "data processing ..."
        print "======================================================================="
        print "Target : "+FolderPath.split("/")[-1]
        print "======================================================================="
        all_data = list()
        result_data = list()
        
        counter = 0
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                counter += 1
                OneDayList = list()
                with open(os.path.join(tops,file), "r") as fp:
                    for line in fp:
                        #Get data from string
                        line=line.replace("\n","")
                        line=line.replace("\r","")
                        OneDayList.append(line)
                    print "progress rate: "+str(counter)+" / "+ str(len(files))
                    all_data.append(OneDayList)
                    print "num of all data : ",len(OneDayList)
        
        result_data = all_data[0]
        print "Day 1 : ",len(result_data)
        for i in range(1,len(all_data)):
            result_data = set(all_data[i]) & set(result_data)
            print "Day",i+1,": ",len(result_data)
        

        print "saving data ..."
        result_data = list(result_data)
        result_data = self.restructureList2SaveFormat(result_data)
        self.saveData(SavePath, FileName, result_data)

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)

    def countNumOfColumn(self, FolderPath, selectColumns, Conditions):
        tStart = time.time()
        # initialize dict of selected columns
        dataDict = dict()
        for column in SelectColumns:
            dataDict[column] = []
        
        counter = 0
        total_file_count = 0

        print "parse traffic Log File ..."
        print "======================================================================="
        print "Target : "+FolderPath
        print "======================================================================="
        # parse all log file under FolderPath
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                total_file_count += 1
                FilePath = os.path.join(tops,file)

                with open(FilePath, "r") as sourceFile:
                    for line in sourceFile:
                        #Get data from string
                        line=line.replace("\n","")
                        data=re.split(r"\t",line)

                        #Convert data to dictionary type
                        row=dict()
                        for i in xrange(len(self.fieldName)):
                            row[self.fieldName[i]]=data[i]
                    
                        # check conditions
                        match = 0
                        for key in Conditions.keys():
                            if row.get(key) == Conditions.get(key):
                                match += 1
                    
                        # pick matching data
                        if match == len(Conditions.keys()):
                            counter += 1

                if total_file_count%10 == 0:
                    print "progress rate: "+str(total_file_count)+" / "+ str(len(files))
            print "progress rate: "+str(total_file_count)+" / "+ str(len(files))
        return counter

    def reduceDataSet(self, FolderPath, SelectColumns, Conditions, SavePath):
        tStart = time.time()
        
        print "reduce traffic Log File ..."
        print "======================================================================="
        print "Target : "+FolderPath
        print "======================================================================="

        counter = 0
        total_file_count = 0
        DataList = []
        Fieldnames = SelectColumns + Conditions.keys()

        # parse all log file under FolderPath
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                counter += 1
                FilePath = os.path.join(tops,file)
                FileName = "traffic-log_"+str(counter)+".csv"
                csv_path = os.path.join(SavePath, FileName)

                with open(FilePath, "r") as sourceFile, open(csv_path,"w") as csvfile:
                    # csv file setting
                    writer = csv.DictWriter(csvfile, fieldnames=Fieldnames)
                    writer.writeheader()

                    for line in sourceFile:
                        #Get data from string
                        line=line.replace("\n","")
                        data=re.split(r"\t",line)

                        #Convert data to dictionary type
                        row=dict()
                        for i in xrange(len(self.fieldName)):
                            if self.fieldName[i] in SelectColumns:
                                row[self.fieldName[i]]=data[i]
                            elif self.fieldName[i] in Conditions.keys():
                                row[self.fieldName[i]]=data[i]
                    
                        # check conditions
                        match = 0
                        for key in Conditions.keys():
                            if row.get(key) == Conditions.get(key):
                                match += 1
                        #print row

                        # pick matching data
                        if match == len(Conditions.keys()):
                            writer.writerow(row)
                            #print row
                print "progress rate: "+str(counter)+" / "+ str(len(files))

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)
    
    ##### filterLightDataFromList(FolderPath, Fieldnames, MatchList, ListCol, SavePath, FileName)
    # filter Light Data (the files was made in reduceDataSet function) in specific list out,
    # and save in folder
    #
    # @FolderPath: (str) the target folder path
    # @Fieldnames: (list) the columns of csv file
    # @MatchList: (list) filter codition is in this list, if data match the list will be pick
    # @ListCol: (str) pick column
    # @SavePath: (str) the path is for saving file
    # @FileName: (str) a part of saving files' name
    #
    ######################################################
    def filterLightDataFromList(self, FolderPath, Fieldnames, MatchList, ListCol, SavePath, FileName):
        tStart = time.time()
        
        print "filter light traffic Log File ..."
        print "======================================================================="
        print "Target : "+FolderPath
        print "======================================================================="
                
        total_file_count = 0
        total_traffic_count = 0
        DataList = []
        MatchList = set(MatchList)

        # parse all log file under FolderPath
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                FilePath = os.path.join(tops,file)
                total_file_count += 1
                counter = 0
                
                csv_filename = FileName+"-"+str(total_file_count)+".csv"
                csv_path =  os.path.join(SavePath, csv_filename)

                with open(FilePath, "r") as sourceFile, open(csv_path, "w") as csvfile:
                    # csv file setting
                    reader = csv.DictReader(sourceFile, fieldnames=Fieldnames)
                    writer = csv.DictWriter(csvfile, fieldnames=Fieldnames)

                    for row in reader:
                        temp = [row.get(ListCol)]
                        if set(temp) & MatchList:
                            writer.writerow(row)
                            counter += 1

                total_traffic_count += counter
                print "progress rate: "+str(total_file_count)+" / "+ str(len(files))+" num: "+str(counter)
        
        print "total traffic num: ",total_traffic_count
        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)

    ##### getRequestNum(FolderPath, Fieldnames, ListCol)
    # get the FolderPath files(ex. white-traffic-log-0505) request num in dict format
    #
    # @FolderPath: (str) the target folder path
    # @Fieldnames: (list) the columns of csv file
    # @ListCol: (str) select column for counting
    #
    ######################################################
    def getRequestNum(self, FolderPath, Fieldnames, ListCol):
        tStart = time.time()

        print "get request num from light traffic Log File ..."
        print "======================================================================="
        print "Target : "+FolderPath
        print "======================================================================="
        
        total_file_count = 0
        RankDict = dict()

        # parse all log file under FolderPath
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                counter = 0
                total_file_count += 1
                FilePath = os.path.join(tops,file)

                with open(FilePath, "r") as sourceFile:
                    # csv file setting
                    reader = csv.DictReader(sourceFile, fieldnames=Fieldnames)
                    for row in reader:
                        key = row.get(ListCol)
                        value = RankDict.get(key)
                        if value != None:
                            RankDict.update({key: value+1})
                        else:
                            RankDict.update({key: 1})
                        counter += 1
                print "progress rate: "+str(total_file_count)+" / "+ str(len(files))+" num: "+str(counter)

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)
        return RankDict
    
    ##### rankRequest(FolderPathList, Fieldnames, ListCol, SavePath, FileName)
    # read files under FolderPath in FolderPathList, merge all dict and rank by request number
    # and save the result
    #
    # @FolderPathList: (list) have many FolderPath for getRequestNum() function
    # @Fieldnames: (list) the columns of csv file
    # @ListCol: (str) select column for counting
    # @SavePath: (str) the path is for saving file
    # @FileName: (str) saving file name
    #
    ######################################################
    def rankRequest(self, FolderPathList, Fieldnames, ListCol, SavePath, FileName):
        tStart = time.time()
        row = ""
        rank_data = []
        Total_Rank = dict()

        for i in range(len(FolderPathList)):
            temp_dict = self.getRequestNum(FolderPathList[i], Fieldnames, ListCol)
            Total_Rank = dict(Counter(temp_dict) + Counter(Total_Rank))

        Total_Rank = sorted(Total_Rank.iteritems(), key= lambda d:d[1], reverse = True)
        
        for item in Total_Rank:
            row = str(item[0])+" "+str(item[1])
            rank_data.append(row)
    
        rank_data = self.restructureList2SaveFormat(rank_data)
        self.saveData(SavePath, FileName, rank_data)

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)

    def getConnectionType(self, FolderPath, Fieldnames, ListCol, DataDict):
        tStart = time.time()
        print "get connection type from light traffic Log File ..."
        print "======================================================================="
        print "Target : "+FolderPath
        print "======================================================================="
        
        total_file_count = 0
        connection = []

        # parse all log file under FolderPath
        for tops, dirs, files in os.walk(FolderPath):
            for file in files:
                counter = 0
                total_file_count += 1
                FilePath = os.path.join(tops,file)

                with open(FilePath, "r") as sourceFile:
                    # csv file setting
                    reader = csv.DictReader(sourceFile, fieldnames=Fieldnames)
                    for row in reader:
                        ConnectionType = row.get(ListCol)
                        ID = row.get('DeviceID')

                        if ConnectionType in DataDict.get(ID).keys():
                            DataDict.get(ID).update({ConnectionType:DataDict.get(ID).get(ConnectionType)+1})
                        else:
                            #print type(ConnectionType)
                            DataDict.get(ID).update({ConnectionType:1})

                print "progress rate: "+str(total_file_count)+" / "+ str(len(files))

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)
        return DataDict
    
    def readConnectionTypeResult2Dict(self, FilePath):
        tStart = time.time()
        print "======================================================================="
        print "load connection type and request rank data ..."
        print "======================================================================="
        dataList = []
        ConnectionType = []
        with open(FilePath, "r") as sourceFile:
            
            for row in sourceFile:
            	item_dict = dict()
                row = row.replace("\r\n","")
                row = row.split(" ")
                row.pop(-1)
                item_dict.update({row[0]:{}})

                for content in row[1:]:
                    content = content.split(":")
                    item_dict.get(row[0]).update({content[0]:content[1]})
                    if not content[0] in ConnectionType:
                        ConnectionType.append(content[0])
                dataList.append(item_dict)
        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)
        return dataList, ConnectionType

    def countConnectionTypeRate(self, Datalist, rate, Connection):
    	tStart = time.time()
    	print "======================================================================="
    	print "count Connection type rate ..."
        print "======================================================================="
        cut_point = int(rate * len(Datalist))
        front = Datalist[:cut_point]
        rear = Datalist[cut_point:]
        wifi = 0
        ethernet = 0
        Cellular = 0
        Request = 0
        counter = 0

        for row in front:
            counter += 1
            for data in row.values():
                for key in data.keys():
                    if Connection[key] == 'Wifi':
                        wifi += int(data.get(key))
                    elif Connection[key] == 'Cellular Unknown':
                        Cellular += int(data.get(key))
                    elif Connection[key] == 'Request':
                        Request += int(data.get(key))
                    elif Connection[key] == 'Ethernet Unknown':
                        ethernet += int(data.get(key))
            if counter % 100 == 0:
                print "progress rate: "+str(counter)+" / "+ str(len(front))
        print "progress rate: "+str(counter)+" / "+ str(len(front))
        
        print "======================================================================="
        print "front rate: ",rate
        print "======================================================================="
        print "Wifi : ",wifi ,format(float(wifi)/Request,'0.2%')
        print "Cellular Unknown : ",Cellular ,format(float(Cellular)/Request,'0.2%')
        print "Ethernet Unknown : ",ethernet ,format(float(ethernet)/Request,'0.2%')
        print "Total Request : ", Request
        print "======================================================================="
        
        wifi = 0
        ethernet = 0
        Cellular = 0
        Request = 0
        counter = 0

        for row in rear:
            counter += 1
            for data in row.values():
                for key in data.keys():
                    if Connection[key] == 'Wifi':
                        wifi += int(data.get(key))
                    elif Connection[key] == 'Cellular Unknown':
                        Cellular += int(data.get(key))
                    elif Connection[key] == 'Request':
                        Request += int(data.get(key))
                    elif Connection[key] == 'Ethernet Unknown':
                        ethernet += int(data.get(key))
            if counter % 100 == 0:
                print "progress rate: "+str(counter)+" / "+ str(len(rear))
        print "progress rate: "+str(counter)+" / "+ str(len(rear))

        print "======================================================================="
        print "remain rate: ",1-rate
        print "======================================================================="
        print "Wifi : ",wifi ,format(float(wifi)/Request,'0.2%')
        print "Cellular Unknown : ",Cellular ,format(float(Cellular)/Request,'0.2%')
        print "Ethernet Unknown : ",ethernet ,format(float(ethernet)/Request,'0.2%')
        print "Total Request : ", Request
        print "======================================================================="

        tEnd = time.time()
        print "func: parseTrafficLog2Dict() cost %f sec" % (tEnd - tStart)


if __name__ == '__main__':
    FileStruct = ['Timestamp','ClickID','BidPrice','SellerID','DeviceID','SiteID','PlacementID','IP','Path','BiddingEngineLog','ImpsType','ImpsWidth','ImpsHeight','UserAgent','Language',
                  'Gaid','GaidMd5','Idfa','IdfaMd5','Imei','ImeiMd5','MacMd5','AndroidID','AndroidIDMd5','CookieID','ConnectionType']
    worker = TrafficLogProcessing(FileStruct)
    '''
    ### unzip part
    #================================================================================
    FolderList = ['traffic-log-2017-05-06', 'traffic-log-2017-05-07']
    worker.unzipFile("/ext_storage/commercial_data_0526", FolderList)
    #================================================================================
    '''
    '''
    ### get traffic log data
    #================================================================================
    FolderPath = "/ext_storage/commercial_data_0526/traffic-log-2017-05-07"
    selectColumns = ['DeviceID']
    Conditions = {'SellerID': '8'}
    #selectColumns = ['DeviceID','IP']
    #Conditions = {'SellerID': '8','ConnectionType':'1'}
    filterType = "non-repeat"
    SaveUnit = 10
    SavePath = "/ext_storage/commercial_data_0526/traffic-log-2017-05-07-DeviceID"

    worker.parseTrafficLog2Dict(FolderPath, selectColumns, Conditions, filterType, SaveUnit, SavePath)
    #================================================================================
    '''
    '''
    ### process data of 1 day
    #================================================================================
    FolderPath = "/ext_storage/commercial_data_0526/traffic-log-2017-05-06-DeviceID"
    SavePath = "/ext_storage/commercial_data_0526/traffic-log-2017-05-06-DeviceID"
    FileName = "2017-05-06-DeviceID.txt"
    worker.mergeData(FolderPath, SavePath, FileName)
    #================================================================================
    '''
    '''
    ### process cross day
    #================================================================================
    FolderPath = "/ext_storage/commercial_data_0526/DeviceID-0504-0507"
    SavePath = "/ext_storage/commercial_data_0526/DeviceID-0504-0507"
    FileName = "2017-0506-0507-WhiteDeviceID.txt"
    worker.selectAppearCrossDayData(FolderPath, SavePath, FileName)
    #================================================================================
    '''

    ### count Number of column
    #================================================================================
    #FolderPath = "/ext_storage/commercial_data_0526/DeviceID-0504-0507"
    #selectColumns = ['DeviceID']

    #================================================================================
    '''
    ### reduce traffic log data
    #================================================================================
    #FolderPath = "/ext_storage/commercial_data_0526/traffic-log-2017-05-06"
    #FolderPath = "/media/wmlab/Transcend/traffic-log-2017-05-04"
    SelectColumns = ['DeviceID', 'ConnectionType', 'SiteID']
    Conditions = {'SellerID': '8'}
    #SavePath = "/ext_storage/commercial_data_0526/light-traffic-log-0504"
    #worker.reduceDataSet(FolderPath, SelectColumns, Conditions, SavePath)
    
    SavePathList = ['/ext_storage/commercial_data_0526/light-traffic-log-0504',
                    '/ext_storage/commercial_data_0526/light-traffic-log-0505',
                    '/ext_storage/commercial_data_0526/light-traffic-log-0506',
                    '/ext_storage/commercial_data_0526/light-traffic-log-0507']
    
    FolderPathList = ['/media/wmlab/Transcend/traffic-log-2017-05-04',
                      '/media/wmlab/Transcend/traffic-log-2017-05-05',
                      '/ext_storage/commercial_data_0526/traffic-log-2017-05-06',
                      '/ext_storage/commercial_data_0526/traffic-log-2017-05-07']
    
    for i in range(4):
        worker.reduceDataSet(FolderPathList[i], SelectColumns, Conditions, SavePathList[i])

    #================================================================================
    '''

    '''
    ### Find request and connection type counting in white list
    ### Filter light data
    #================================================================================
    ### load white list
    FolderPath = "/ext_storage/commercial_data_0526/baido-light-data/"
    #SelectColumns = ['DeviceID', 'ConnectionType', 'SiteID']
    FileName = "2017-0506-0507-WhiteDeviceID.txt"
    WhiteList = worker.loadData(FolderPath, FileName)
    print len(WhiteList)
    #================================================================================
    ### filter data
    SavePathList = ['/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0504',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0505',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0506',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0507']
    
    FolderPathList = ['/ext_storage/commercial_data_0526/baido-light-data/light-traffic-log-0504',
                      '/ext_storage/commercial_data_0526/baido-light-data/light-traffic-log-0505',
                      '/ext_storage/commercial_data_0526/baido-light-data/light-traffic-log-0506',
                      '/ext_storage/commercial_data_0526/baido-light-data/light-traffic-log-0507']
    #SavePath = "/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0504"
    FileName = "WhiteTraffic"

    #FolderPath = "/ext_storage/commercial_data_0526/baido-light-data/light-traffic-log-0504"
    ListCol = 'DeviceID'
    Fieldnames = ['DeviceID', 'ConnectionType', 'SiteID','SellerID']
    for i in range(len(FolderPathList)):
        worker.filterLightDataFromList(FolderPathList[i], Fieldnames, WhiteList, ListCol, SavePathList[i], FileName)
    #================================================================================
    '''
    '''
    ### get White List traffic log Request Num
    #================================================================================
    ListCol = 'DeviceID'
    FolderPathList = ['/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0504',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0505',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0506',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0507']
    SavePath = "/ext_storage/commercial_data_0526/baido-light-data"
    FileName = "white-request-rank-data-2017-0504-0507.txt"
    Fieldnames = ['DeviceID', 'ConnectionType', 'SiteID','SellerID']
    Total_Rank = dict()

    worker.rankRequest(FolderPathList, Fieldnames, ListCol, SavePath, FileName)
    #================================================================================
    '''
    '''
    ### get White List traffic log Connection Type
    #================================================================================
    # get device ID dict
    FilePath = '/ext_storage/commercial_data_0526/baido-light-data'
    FileName = 'white-request-rank-data-2017-0504-0507.txt'
    DataList = worker.loadData(FilePath, FileName)

    DataDict = dict()
    for item in DataList:
        DeviceID = item.split(" ")[0]
        DataDict[DeviceID] = {'Request':int(item.split(" ")[1])}
    

    #================================================================================
    FolderPath = '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0504'
    Fieldnames = ['DeviceID', 'ConnectionType', 'SiteID','SellerID']
    ListCol = 'ConnectionType'
    
    FolderPathList = ['/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0504',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0505',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0506',
                    '/ext_storage/commercial_data_0526/baido-light-data/white-traffic-log-0507']

    Total_Rank = dict()

    for i in range(len(FolderPathList)):
        worker.getConnectionType(FolderPathList[i], Fieldnames, ListCol, DataDict)
    
    DataDict = sorted(DataDict.iteritems(), key= lambda d:int(d[1].get('Request')), reverse = True)
    
    temp = ""
    row = ""
    rank_data = []
    for item in DataDict:
        item_temp = ""
        for key in item[1].keys():
            temp = key +":"+str(item[1].get(key))+" "
            item_temp += temp

        row = str(item[0])+" "+item_temp
        rank_data.append(row)
    
    SavePath = "/ext_storage/commercial_data_0526/baido-light-data"
    FileName = "white-request-rank-ConnetionType-data-2017-0504-0507.txt"
    rank_data = worker.restructureList2SaveFormat(rank_data)
    worker.saveData(SavePath, FileName, rank_data)
    #================================================================================
    '''
    '''
    ### count avg and standard deviation
    #================================================================================
    # get device ID dict
    FilePath = '/ext_storage/commercial_data_0526/baido-light-data'
    FileName = 'white-request-rank-data-2017-0504-0507.txt'
    DataList = worker.loadData(FilePath, FileName)
    
    DataDict = dict()
    for item in DataList:
        DeviceID = item.split(" ")[0]
        DataDict[DeviceID] = {'Request':int(item.split(" ")[1])}


    
    # average
    total_request = [i.get('Request') for i in DataDict.values()]
    total_request = sorted(total_request, reverse=True)
    

    index = int(len(total_request) * 0.1)
    total_request = total_request[index:]
    print len(total_request)
    print total_request[0]
    print total_request[-1]

    avg = float(sum(total_request))/len(total_request)
    print "average: ",avg
    # standard deviation
    print "standard deviation: ",np.std(total_request)

    # standard deviation
    temp = 0
    for i in total_request:
        temp += (i - avg)**2
    temp = float(temp) / len(total_request)
    temp = temp ** 0.5
    print "standard deviation: ",temp
    #================================================================================
    '''
    '''
    # read ConnectionType & Request rank File to Dict
    #================================================================================
    FilePath = '/ext_storage/commercial_data_0526/baido-light-data/white-request-rank-ConnetionType-data-2017-0504-0507.txt'
    Datalist, ConnectionType = worker.readConnectionTypeResult2Dict(FilePath)

    rate = 0.1
    Connection = {'0,6':'Ethernet Unknown', '2,3':'Cellular Unknown', '2,4':'Cellular Unknown', '1':'Wifi', 'Request':'Request'}
    worker.countConnectionTypeRate(Datalist, rate, Connection)
    #================================================================================
    '''
    '''
    # merge rank data
    #================================================================================
    FolderPath = '/media/wmlab/Transcend/request'
    SavePath = '/media/wmlab/Transcend/request'
    FileName = 'Request-rank-0504.txt'
    #worker.mergeData(FolderPath, SavePath, FileName)
    #================================================================================
    DataDict = dict()
    counter = 0
    with open(os.path.join(SavePath,FileName),'r') as fp:
        for row in fp:
            counter += 1
            row = row.replace('\r\n','')
            row = row.split(' ')
            if DataDict.get(row[0]) == None:
                DataDict.update({row[0]:int(row[1])})
            else:
                DataDict.update({row[0]: int(DataDict.get(row[0]))+int(row[1])})
            if counter % 100 == 0:
                print "progress rate: "+str(counter)
        print "progress rate: "+str(counter)

    print len(DataDict)
    DataDict = sorted(DataDict.iteritems(), key= lambda d:d[1], reverse = True)

    rank_data = []
    for item in DataDict:
        row = str(item[0])+" "+str(item[1])
        rank_data.append(row)

    print len(rank_data)

    # average
    total_request = [i for i in DataDict]
    
    # 10%
    index = int(len(total_request) * 0.1)
    total_request = [i[1] for i in total_request[:index]]

    #print sum(total_request)
    print "Max: ",total_request[0]
    print "Min: ",total_request[-1]
    print "Sum: ",sum(total_request)
    avg = float(sum(total_request))/len(total_request)
    print "average: ",avg
    # standard deviation
    print "standard deviation: ",np.std(total_request)

    SavePath = "/media/wmlab/Transcend/request"
    FileName = "request-rank-data-2017-0504.txt"
    #rank_data = worker.restructureList2SaveFormat(rank_data)

    #worker.saveData(SavePath, FileName, rank_data)
    '''






