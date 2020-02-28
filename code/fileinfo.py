class fileinfo:
    def __init__(self, filename, filesize, numchunks, ownerdic):
        self.filename = filename
        self.filesize = filesize
        self.numchunks = numchunks
        self.ownerdic = ownerdic #key: peer (port ip), value: list of chunkid (interval)
    
    @staticmethod
    def create_fileinfo(filename, filesize, numchunks, ownerdic):
        return fileinfo(filename, filesize, numchunks, ownerdic)
    
    def update_ownerdic(self, peerinfo, chunkid):
        # flag = False
        # for peer in self.ownerdic:
        #     if peer[0] == peerinfo[0] and peer[1] == peerinfo[1]:
        #         flag = True
        #         break
        if peerinfo in self.ownerdic:
            modified = False
            listofintervals = self.ownerdic[peerinfo]
            for interval in listofintervals:
                if interval[0] > chunkid+1 or interval[1] < chunkid-1:
                    continue
                else:
                    if interval[0] > chunkid:
                        interval[0] = chunkid
                    if interval[1] < chunkid:
                        interval[1] = chunkid
                    modified = True
                    break
            if modified == False:
                listofintervals.append([chunkid, chunkid])
            listofintervals.sort(key = lambda x:x[0])

            result_intervals = []
            for interval in listofintervals:
                if len(result_intervals) != 0 and result_intervals[-1][1]+1 == interval[0]:
                    result_intervals[-1][1] = interval[1]
                else:
                    result_intervals.append(interval)            
            self.ownerdic[peerinfo] = result_intervals
        else:
            self.ownerdic[peerinfo] = [[chunkid, chunkid]]

    def delete_peer(self, peer):
        if peer in self.ownerdic:
            self.ownerdic.pop(peer)
    
    def count_chunk(self, peer):
        counter = 0
        listofinterval = (self.ownerdic)[peer]
        for interval in listofinterval:
            low = interval[0]
            high = interval[1]
            counter += (high-low+1)
        return counter