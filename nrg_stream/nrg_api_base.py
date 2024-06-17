import http.client
import json
import time
import csv
from datetime import datetime
from datetime import timedelta
import re
import pandas as pd
import os
import zipfile
import certifi
import ssl


class NRGStreamApi:

    def __init__(self,username=None,password=None):
        self.username = "Spencer2"
        self.password = "PwR4Spencer2"
        self.server = 'api.nrgstream.com'        
        self.tokenPath = '/api/security/token'
        self.releasePath = '/api/ReleaseToken'
        self.tokenPayload = f'grant_type=password&username={self.username}&password={self.password}'
        self.tokenExpiry = datetime.now() - timedelta(seconds=60)
        self.accessToken = ""        

    def getToken(self):
        try:
            if self.isTokenValid() == False:                             
                headers = {"Content-type": "application/x-www-form-urlencoded"}      
                # Connect to API server to get a token
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)
                conn.request('POST', self.tokenPath, self.tokenPayload, headers)
                res = conn.getresponse()                
                res_code = res.code
                # Check if the response is good

                if res_code == 200:
                    res_data = res.read()
                    # Decode the token into an object
                    jsonData = json.loads(res_data.decode('utf-8'))
                    self.accessToken = jsonData['access_token']                         
                    # Calculate new expiry date
                    self.tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])                        
                    # print('token obtained')
                    # print(self.accessToken)
                else:
                    res_data = res.read()
                    print(res_data.decode('utf-8'))
                conn.close()                          
        except Exception as e:
            print("getToken: " + str(e))
            # Release token if an error occured
            self.releaseToken()      

    def releaseToken(self):
        try:            
            headers = {}
            headers['Authorization'] = f'Bearer {self.accessToken}'            
            context = ssl.create_default_context(cafile=certifi.where())
            conn = http.client.HTTPSConnection(self.server,context=context)
            conn.request('DELETE', self.releasePath, None, headers)  
            res = conn.getresponse()
            res_code = res.code
            if res_code == 200:   
                # Set expiration date back to guarantee isTokenValid() returns false
                self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                # print('token released')
        except Exception as e:
            print("releaseToken: " + str(e))

    def isTokenValid(self):
        if self.tokenExpiry==None:
            return False
        elif datetime.now() >= self.tokenExpiry:            
            return False
        else:
            return True            

    def GetStreamDataByStreamId(self,streamIds, fromDate, toDate, dataFormat='csv', dataOption=''):
        stream_data = "" 
        # Set file format to csv or json
        DataFormats = {}
        DataFormats['csv'] = 'text/csv'
        DataFormats['json'] = 'Application/json'

        try:                            
            for streamId in streamIds:            
                # Get an access token
                self.getToken()    
                if self.isTokenValid():
                    # Setup the path for data request. Pass dates in via function call
                    path = f'/api/StreamData/{streamId}'
                    if fromDate != '' and toDate != '':
                        path += f'?fromDate={fromDate.replace(" ", "%20")}&toDate={toDate.replace(" ", "%20")}'
                    if dataOption != '':
                        if fromDate != '' and toDate != '':
                            path += f'&dataOption={dataOption}'        
                        else:
                            path += f'?dataOption={dataOption}'        

                    # Create request header
                    headers = {}            
                    headers['Accept'] = DataFormats[dataFormat]
                    headers['Authorization']= f'Bearer {self.accessToken}'

                    # Connect to API server
                    context = ssl.create_default_context(cafile=certifi.where())
                    self.path = path
                    conn = http.client.HTTPSConnection(self.server,context=context)
                    conn.request('GET', path, None, headers)
                    res = conn.getresponse()        
                    res_code = res.code                    
                    if res_code == 200:   
                        try:
                            print(f'{datetime.now()} Outputing stream {path} res code {res_code}')
                            # output return data to a text file
                            if dataFormat == 'csv':
                                stream_data += res.read().decode('utf-8').replace('\r\n','\n') 
                            elif dataFormat == 'json':
                                stream_data += json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                            conn.close()

                        except Exception as e:
                            print(str(e))            
                            self.releaseToken()
                            return None  
                    else:
                        print(str(res_code) + " - " + str(res.reason) + " - " + str(res.read().decode('utf-8')))

                self.releaseToken()   
                # Wait 1 second before next request
                time.sleep(1)
            return stream_data        
        except Exception as e:
            print(str(e))    
            self.releaseToken()
            return None

    def GetStreamDataByFolderId(self,folderId, fromDate, toDate, dataFormat='csv'):
        try:        
            # Get an access token
            self.getToken()    
            if self.isTokenValid():                 
                # Setup the path for data request.
                path = f'/api/StreamDataUrl?folderId={folderId}' # folderId is 

                # Create request header
                headers = {}
                headers['Accept'] = 'text/csv'
                headers['Authorization'] = f'Bearer {self.accessToken}'
                # Connect to API server
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)
                conn.request('GET', path, None, headers)
                res = conn.getresponse()                
                self.releaseToken()    
                # Process data returned from request to obtain Stream Ids of the folder constituents
                streamUrls=(res.read().decode('utf-8'))
                streamLines=streamUrls.split("\n")        
                streamIds = []
                for streamLine in streamLines[6:]:
                    # Process the rest of the streamLines
                    streamId = streamLine.split(',')[0]
                    if streamId != '':
                        streamIds.append(int(streamId))

                # Get data for individual streams
                self.GetStreamDataByStreamId(streamIds, fromDate, toDate, dataFormat)

        except Exception as e:
            print(str(e))    
            self.releaseToken()
            return None        

    def ListGroupExtracts(self, dataFormat='csv'):
        try:        
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            # Get an access token
            self.getToken()    
            if self.isTokenValid():                   
                # Setup the path for data request.
                path = f'/api/ListGroupExtracts'                

                # Create request header
                headers = {}   
                headers['Accept'] = DataFormats[dataFormat]                    
                headers['Authorization'] = f'Bearer {self.accessToken}'                
                # Connect to API server
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)                
                conn.request('GET', path, None, headers)

                res = conn.getresponse()                
                if dataFormat == 'csv':
                    groupExtractsList = res.read().decode('utf-8').replace('\r\n','\n') 
                elif dataFormat == 'json':
                    groupExtractsList = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                self.releaseToken()                

                return groupExtractsList
        except Exception as e:
            print("ListGroupExtracts: " + str(e))    
            self.releaseToken()
            return None  

    def StreamDataOptions(self, streamIds, dataFormat='csv'):
        try:      
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            resultSet = {}
            for streamId in streamIds:
                # Get an access token
                if streamId not in resultSet:
                    self.getToken()                        
                    if self.isTokenValid():                 
                        # Setup the path for data request.
                        path = f'/api/StreamDataOptions/{streamId}'                        
                        # Create request header
                        headers = {}     
                        headers['Accept'] = DataFormats[dataFormat]                                   
                        headers['Authorization'] = f'Bearer {self.accessToken}'
                        # Connect to API server
                        context = ssl.create_default_context(cafile=certifi.where())
                        conn = http.client.HTTPSConnection(self.server,context=context)
                        conn.request('GET', path, None, headers)
                        res = conn.getresponse()
                        self.releaseToken()       
                        if dataFormat == 'csv':
                            resultSet[streamId] = res.read().decode('utf-8').replace('\r\n','\n') 
                        elif dataFormat == 'json':
                            resultSet[streamId] = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)                            
                    time.sleep(1)                        
            return resultSet            
        except Exception as e:
            print(str(e))    
            self.releaseToken()
            return None          

    def GetGroupExtractHeader(self, filePrefix):        
        try:                    
            # Get an access token
            self.getToken()                
            if self.isTokenValid():                 
                # Setup the path for data request.
                path = f'/api/GroupExtractHeader/{filePrefix}'                        
                # Create request header
                headers = {}                                
                headers['Authorization'] = f'Bearer {self.accessToken}'
                # Connect to API server
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)
                conn.request('GET', path, None, headers)
                res = conn.getresponse()                
                self.releaseToken()                  
                groupExtractHeader = (res.read().decode('utf-8'))    
                return groupExtractHeader                            
        except Exception as e:
            print(str(e))    
            self.releaseToken()
            return None    

    def GetGroupExtract(self, filePrefix, fileDate, fileSavePath):                
        try:                    
            # Get an access token
            self.getToken()    
            return_data = []
            return_data.append(datetime.now())
            if self.isTokenValid():   

                # Setup the path for data request.
                path = f'/api/GroupExtract/{filePrefix}?fileDate={fileDate}'                        
                # Create request header
                headers = {}                
                headers['Authorization'] = f'Bearer {self.accessToken}'
                # Connect to API server
                print(path)
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)
                conn.request('POST', path, None, headers)
                res = conn.getresponse()    
                data = res.read()                
                self.releaseToken()
                if(res.status == 200):
                    cd = res.getheader('Content-Disposition')                       
                    # Get filename out of the content disposition in header
                    filename = re.findall('filename=(.+)', cd)                                        
                    with open(fileSavePath + '/' + filename[0], 'wb') as f:
                        f.write(data)
                    return filename[0]
                else:                    
                    return str(res.status) + ' - ' + str(res.reason)
        except Exception as e:            
            self.releaseToken()                        
            return str(e)

    def GetStreamList(self, dataFormat='csv'):
        try:        
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            # Get an access token
            self.getToken()    
            if self.isTokenValid():                   
                # Setup the path for data request.
                path = f'/api/StreamList'                

                # Create request header
                headers = {}   
                headers['Accept'] = DataFormats[dataFormat]                    
                headers['Authorization'] = f'Bearer {self.accessToken}'                
                # Connect to API server
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)                
                conn.request('GET', path, None, headers)

                res = conn.getresponse()                
                if dataFormat == 'csv':
                    streamList = res.read().decode('utf-8').replace('\r\n','\n') 
                elif dataFormat == 'json':
                    streamList = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                self.releaseToken()                

                return streamList
        except Exception as e:
            print("StreamList: " + str(e))    
            self.releaseToken()
            return None

    def GetFolderList(self, dataFormat='csv'):
        try:        
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            # Get an access token
            self.getToken()    
            if self.isTokenValid():                   
                # Setup the path for data request.
                path = f'/api/FolderList'                

                # Create request header
                headers = {}   
                headers['Accept'] = DataFormats[dataFormat]                    
                headers['Authorization'] = f'Bearer {self.accessToken}'                
                # Connect to API server
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)     
                conn.request('GET', path, None, headers)

                res = conn.getresponse()
                if dataFormat == 'csv':
                    folderList = res.read().decode('utf-8').replace('\r\n','\n') 
                elif dataFormat == 'json':
                    folderList = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                self.releaseToken()                

                return folderList
        except Exception as e:
            print("FolderList: " + str(e))    
            self.releaseToken()
            return None        

    def csvStreamToPandas(self, streamData):
        # split lines of return string from api
        streamData = streamData.split("\n")

        # remove empty elements from list
        streamData = [x for x in streamData if len(x) > 0] 

        # remove header data
        streamData = [x for x in streamData if x[0] != '#'] 

        # split elements into lists of lists
        streamData = [x.split(",") for x in streamData] 

        # create dataframe
        df = pd.DataFrame(streamData[1:], columns=streamData[0]) 

        return df

try:    
    # Authenticate with your NRGSTREAM username and password contained in credentials.txt, file format = username,password 
    f = open("credentials.txt", "r")
    credentials = f.readline().split(',')
    f.close()
    nrgStreamApi = NRGStreamApi(credentials[0],credentials[1])         
    # Date range for your data request
    # Date format must be 'mm/dd/yyyy hh:ss'
    fromDateStr ='02/11/2021'
    toDateStr = '02/11/2021'

    # Specify output format - 'csv' or 'json'
    dataFormat = 'csv'
    
    # Convert streams to Pandas dataframes
    # Only compatible with getByStream and getByFolder
    dataFrameConvert = False
    
    # Data Option
    dataOption = ''
    
    # Output from the API request is written to the stream_data variable
    stream_data = ""

    # Output from the API request is written to the streamList variable
    streamList = ""
    
    # Output from the API request is written to the folderList variable
    folderList = ""
    
    # Output from the API request is written to the groupExtractsList variable
    groupExtractsList = ""
    
    # Change to True to get streams from output of calling 'Generate API' in NRGStream Trader desktop application
    getByGenAPIOutput= False
    if getByGenAPIOutput:        
        streamIds = []
        with open('GenAPIOutput.csv', newline='') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:                         
                if '#' not in row[0] and row[0].isdigit():         
                    streamIds.append(row[0])                          
        stream_data = nrgStreamApi.GetStreamDataByStreamId(streamIds, fromDateStr, toDateStr, dataFormat, dataOption)        
       
    # Change to True to get streams by Stream Id
    getByStream = True    
    if getByStream:
        # Pass in individual stream id
        streamIds = [112403, 3, 17]              
        # Or pass in list of stream ids
        #streamIds = [139308, 3, 225, 4117, 17, 545, 40034]         
        stream_data = nrgStreamApi.GetStreamDataByStreamId(streamIds, fromDateStr, toDateStr, dataFormat, dataOption) 
        
        if(dataFrameConvert and dataFormat == 'csv'):
            if(len(streamIds) > 1):
                print('Please only convert 1 stream to a Pandas dataframe at a time')
            else:
                stream_data = nrgStreamApi.csvStreamToPandas(stream_data)
        # print (stream_data)
        
    # Change to True to get streams by Folder Id
    getByFolder = False
    if getByFolder:
        # Pass in individual folder id
        folderId = 9
        nrgStreamApi.GetStreamDataByFolderId(folderId, fromDateStr, toDateStr, dataFormat)

    # Change to True to retrieve a list of data options available for a given stream
    getStreamDataOptions = False
    if getStreamDataOptions:          
        # Pass in a list of streamIds to get the shapes available for each
        # These shapes can be passed to StreamData endpoint as 'displayOption' to retrieve only that shape
        streamIds = [2270]    
        streamDataOptions = nrgStreamApi.StreamDataOptions(streamIds, dataFormat)
        print(streamDataOptions)
        
    # Change to True to retrieve a list of available Group Extracts
    getListGroupExtracts = False
    if getListGroupExtracts:
        # Returns a list of filePrefixes and their descriptions
        # These filePrefixes correspond to available group extracts
        groupExtractsList = nrgStreamApi.ListGroupExtracts(dataFormat)
        print(groupExtractsList)
        
    getGroupExtractHeader = False
    if getGroupExtractHeader:
        # For a list of available file prefixes call the ListGroupExtracts endpoint
        filePrefixes = ['ERCSTL'] # ['CAHASP','PJMHR','MISOHR','ISONEHR','NYHR','SPPDAM','CADAM','PJMDAM','MISODAM','ISONEDAM','NYDAM','ERCDAM','ERCSTL','MISOHREST','MISODAMEST','ERCRTMBUS','MEXDAM','MEXRTM','ERCRTM','ERCDAMBUS']
        for filePrefix in filePrefixes:
            groupExtractHeaders = nrgStreamApi.GetGroupExtractHeader(filePrefix)
            print(filePrefix + ' - ' + groupExtractHeaders)
            time.sleep(1)
    
    getGroupExtract = False
    if getGroupExtract:
        # To correctly retrieve a group extract you must pass a filePrefix and the date you're interested in
        # For a list of available file prefixes call the ListGroupExtracts endpoint
        filePrefixes = ['PJMHR','MISOHR'] # ['CAHASP','PJMHR','MISOHR','ISONEHR','NYHR','SPPDAM','CADAM','PJMDAM','MISODAM','ISONEDAM','NYDAM','ERCDAM','ERCSTL','MISOHREST','MISODAMEST','ERCRTMBUS','MEXDAM','MEXRTM','ERCRTM','ERCDAMBUS']
        fileSavePath = 'GroupExtracts'
        if(os.path.exists(fileSavePath) == False):
            os.mkdir(fileSavePath)
        fileNames = {}
        fileHeaders = {}
        for filePrefix in filePrefixes:
            fromDate = datetime.strptime(fromDateStr, '%m/%d/%Y')
            toDate = datetime.strptime(toDateStr, '%m/%d/%Y')
            groupExtractHeader = nrgStreamApi.GetGroupExtractHeader(filePrefix)
            fileHeaders[filePrefix] = groupExtractHeader
            while fromDate <= toDate:
                fileDate = fromDate.strftime('%m/%d/%Y')
                
                result = nrgStreamApi.GetGroupExtract(filePrefix, fileDate, fileSavePath)
                if('.zip' in result):
                    if(filePrefix in fileNames.keys()):
                        fileNames[filePrefix].append(result)
                    else:
                        fileNames[filePrefix] = [result]
                else:
                    # an error must have occurred
                    print(result)    
                fromDate = fromDate + timedelta(days=1)
                
                time.sleep(1)
                
        #unzip and combine files
        timestr = time.strftime("%Y%m%d-%H%M%S")
        for filePrefix in fileNames:
            allFiles = []
            for fileName in fileNames[filePrefix]:
                filePath = fileSavePath + '/' + fileName                
                zip_ref = zipfile.ZipFile(filePath)  # create zipfile object
                zip_ref.extractall(fileSavePath)  # extract file to current directory                
                zip_ref.close()  # close file
                srcFilePath = filePath.replace('.zip','.csv') 
                allFiles.append(srcFilePath)
            
            outFile = fileSavePath + '/' + filePrefix + '_' + timestr + '.csv'
            file_out = open(outFile, 'w')
            headerAdded = False
            for inFile in allFiles:
                file_in = open(inFile, 'r')
                content = file_in.read()
                if(headerAdded == False):
                    file_out.write(fileHeaders[filePrefix].replace('\n',''))
                    headerAdded = True
                file_out.write(content)
                file_in.close()
            file_out.close()

    # Change to True to retrieve a list of available Streams
    getStreamList = False
    if getStreamList:
        # Returns a list of Streams and their descriptions
        streamList = nrgStreamApi.GetStreamList(dataFormat)        
        print("StreamList successful")        
            
    # Change to True to retrieve a list of available Folders
    getFolderList = False
    if getFolderList:
        # Returns a list of Folders and their descriptions
        folderList = nrgStreamApi.GetFolderList(dataFormat)              
        print("FolderList successful")
        
    # Change to True to write the return data to disk 
    WriteToDisk = True
    if WriteToDisk:
        try:
            if stream_data != None:
                if stream_data != "":
                    if(dataFrameConvert == True):
                        print("Set dataFrameConvert to False to create a text file")
                    else:
                        output_file_name = "streamdata.txt"
                        output_file = open(output_file_name, "w+")        
                        output_file.write(stream_data)
                        output_file.close()
            if streamList != None:
                if streamList != "":                    
                    output_file_name = "streamList.csv"
                    output_file = open(output_file_name, "w+")
                    output_file.write(streamList)
                    output_file.close()
            if folderList != None:
                if folderList != "":                    
                    output_file_name = "folderList.csv"
                    output_file = open(output_file_name, "w+")
                    output_file.write(folderList)
                    output_file.close()                    
            if groupExtractsList != None:
                if groupExtractsList != "":
                    output_file_name = "groupextractslist.txt"
                    output_file = open(output_file_name, "w+")
                    output_file.write(groupExtractsList)
                    output_file.close()   
        except Exception as e:
            print(str(e))      
except Exception as e:
    print(str(e))
