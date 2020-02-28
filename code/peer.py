import socket
from chunk import chunk
from fileinfo import fileinfo
import sys
import os
import pickle
import collections
import threading
import time

#set up variables
alive_chunks = {} #key: filename, value: fileinfo class
owned_chunks = {} #key: filename, value: {key: chunkid, value: owned_chunk class}
identifier = 0
original_file = [] #filename, filesize
complete_files = [] #list of files that have the complete chunks
min_alive_time = 0
start_time = 0
peer_identifier = 0
terminate_flag = False
write_flag = [] #filenames that are already written

owned_chunks_lock = threading.Lock()
alive_chunks_lock = threading.Lock()
stdout_lock = threading.Lock()
complete_files_lock = threading.Lock()

class myThread(threading.Thread):
    def __init__(self, typeConnection, tcpSocket, udpSocket, udpPort, udpIP):
        threading.Thread.__init__(self)
        self.type = typeConnection
        self.tcpSocket = tcpSocket
        self.udpSocket = udpSocket
        self.udpPort = udpPort
        self.udpIP = udpIP
    def run(self):
        if (self.type == "tcp"):
            tcp_connection(self.tcpSocket, self.udpPort, self.udpIP)
        elif (self.type == "udp_send"):
            udp_send_connection(self.tcpSocket, self.udpSocket, self.udpPort, self.udpIP)
        elif (self.type == "udp_receive"):
            udp_receive_connection(self.tcpSocket, self.udpSocket, self.udpPort, self.udpIP)

def combinePeer(udpPort, udpIP):
    return str(udpPort) + " " + udpIP

def get_port(peer):
    splits = peer.split(" ")
    return int(splits[0])

def get_ip(peer):
    splits = peer.split(" ")
    return splits[1]

def find_open_port(sock):
    s = socket.socket(socket.AF_INET, sock)
    s.bind(('', 0))
    port = s.getsockname()[1]
    return port

def setup_chunks():
    files = []
    for (dirpath, dirnames, filenames) in os.walk("./Shared"):
        for f in filenames:
            files.append(os.path.join(dirpath, f))

    for f in files:
        file_data = open(f, "rb").read()
        filesize = len(file_data)
        original_file.append(f)
        original_file.append(filesize)
        numloop = filesize//512 + 1
        pos = 0
        global identifier
        identifier = 0
        owned_chunks_lock.acquire()
        owned_chunks[f] = {}
        for x in range(numloop):
            if x == numloop - 1:
                data = file_data[pos:]
                chunk_owned = chunk.create_owned_chunk(f, identifier, data)
                owned_chunks[f][identifier] = chunk_owned
                identifier += 1
            else:
                data = file_data[pos:pos+512]
                chunk_owned = chunk.create_owned_chunk(f, identifier, data)
                owned_chunks[f][identifier] = chunk_owned
                pos += 512
                identifier += 1
        owned_chunks_lock.release()
        complete_files_lock.acquire()
        complete_files.append(f)
        complete_files_lock.release()

def find_missing_chunks():
    #return {filename, listofintervals}
    result = {}
    alive_chunks_lock.acquire()
    for filename in alive_chunks:
        if filename in complete_files: continue

        numchunks = alive_chunks[filename].numchunks
        
        if filename in owned_chunks:
            temp = [[0, numchunks-1]]
            owned_chunks_lock.acquire()
            for chunkid in owned_chunks[filename]:
                for interval in temp:
                    interval_low = interval[0]
                    interval_high = interval[1]
                    if interval_high == interval_low and interval_high == chunkid:
                        break
                    if chunkid == interval_high:
                        temp.remove(interval)
                        temp.append([interval_low, chunkid-1])
                        break
                    if chunkid == interval_low:
                        temp.remove(interval)
                        temp.append([chunkid+1, interval_high])
                        break
                    if chunkid < interval_high and chunkid > interval_low:
                        temp.remove(interval)
                        temp.append([interval_low, chunkid-1])
                        temp.append([chunkid+1, interval_high])
                        break
            owned_chunks_lock.release()
            result[filename] = temp
        else:
            interval_size = 500
            numloop = numchunks//interval_size + 1
            temp = []
            pos = 0
            for x in range(numloop):
                if x == numloop - 1:
                    temp.append([pos, numchunks-1])
                else:
                    temp.append([pos, pos+interval_size-1])
                    pos += interval_size
            result[filename] = temp
    alive_chunks_lock.release()
    return result

def peer_contains(ownerdic, peer, interval):
    listofintervals = ownerdic[peer]
    for owned_interval in listofintervals:
        interval_low = interval[0]
        interval_high = interval[1]
        owned_interval_low = owned_interval[0]
        owned_interval_high = owned_interval[1]

        if interval_low >= owned_interval_low and interval_high <= owned_interval_high:
            return True
    return False

def update_tracker_disconnect(tcpSocket, udpPort, udpIP):
    disconnect = ["disconnect", udpPort, udpIP]
    disconnect_byte = pickle.dumps(disconnect)
    tcpSocket.send(disconnect_byte)

def udp_receive_connection(tcpSocket, udpSocket, udpPort, udpIP): #receive request/data, and also send data
    while True:
        try:
            message_byte, clientAddress = udpSocket.recvfrom(2048)
            message = pickle.loads(message_byte)

            message_type = message[0]
            if message_type == "request":
                filename = message[1]
                interval = message[2]

                interval_low = interval[0]
                interval_high = interval[1]
                for i in range(interval_low, interval_high+1):
                    data = ["data", owned_chunks[filename][i]]
                    data_byte = pickle.dumps(data)
                    udpSocket.sendto(data_byte, clientAddress)
            elif message_type == "data":
                data = message[1]
                filename = data.file_name
                chunkid = data.identifier

                #write to owned_chunks
                owned_chunks_lock.acquire()
                if filename not in owned_chunks:
                    owned_chunks[filename] = {}
                owned_chunks[filename][chunkid] = data
                owned_chunks_lock.release()

                #tell tracker I have updates
                update = [filename, chunkid, udpPort, udpIP]
                update_byte = pickle.dumps(update)
                tcpSocket.send(update_byte)
                numacquired = len(owned_chunks[filename])
                totalnum = alive_chunks[filename].numchunks

                num_owned_chunks = len(owned_chunks[filename])
                num_chunks_total = alive_chunks[filename].numchunks
                if num_owned_chunks == num_chunks_total and filename not in write_flag:
                    f = open(filename, "ab")
                    for num in range(num_chunks_total):
                        f.write(owned_chunks[filename][num].data)
                    f.close()
                    write_flag.append(filename)

                    complete_files_lock.acquire()
                    if filename not in complete_files:
                        complete_files.append(filename)
                    complete_files_lock.release()

        except socket.timeout:
            update_tracker_disconnect(tcpSocket, udpPort, udpIP)
            global terminate_flag
            terminate_flag = True
            break

def udp_send_connection(tcpSocket, udpSocket, udpPort, udpIP): #request data thread
    while True:
        if terminate_flag == True: break

        missing_chunks = find_missing_chunks()

        if not missing_chunks: continue

        for filename in missing_chunks:
            alive_chunks_lock.acquire()
            fileinfo_class = alive_chunks[filename]
            alive_chunks_lock.release()
            ownerdic = fileinfo_class.ownerdic
            for interval in missing_chunks[filename]:
                for peer in ownerdic:
                    if peer_contains(ownerdic, peer, interval):
                        #send request
                        port = get_port(peer)
                        ip = get_ip(peer)
                        request = ["request", filename, interval]
                        request_byte = pickle.dumps(request)
                        udpSocket.sendto(request_byte,(ip, port))
                        time.sleep(0.5)
                        break

def tcp_connection(peerSocket, udpPort, udpIP):
    #receive identifying integer and current alive chunks from tracker
    receive_info_byte = peerSocket.recv(1024)
    receive_info = pickle.loads(receive_info_byte)
    identifier = receive_info[0]
    global peer_identifier
    peer_identifier = identifier
    global alive_chunks
    alive_chunks = receive_info[1]

    #send available files/chunks info to tracker
    send_initial_chunk_info(peerSocket, udpPort, udpIP)

    #receive tracker broadcast
    while True:
        receive_info_byte = peerSocket.recv(1024)

        try:
            receive_info = pickle.loads(receive_info_byte)
        except:
            continue
        modified_chunks = receive_info[0]
        broad_type = receive_info[1]

        #update alive_chunks
        if broad_type == "fileinfo":
            update_alive_fileinfo(modified_chunks)
        elif broad_type == "disconnect":
            if peer_identifier == modified_chunks[3]:
                break
            else:
                #remove from alive_chunks
                alive_chunks_lock.acquire()
                for filename in alive_chunks:
                    peer = combinePeer(modified_chunks[1], modified_chunks[2])
                    alive_chunks[filename].delete_peer(peer)
                alive_chunks_lock.release()
        elif broad_type == "update":
            update_alive_chunks(modified_chunks)

def send_initial_chunk_info(peerSocket, udpPort, udpIP):
    send_info = [] #list of fileinfo and udpport

    owned_chunks_lock.acquire()
    for filename in owned_chunks:
        temp = []
        filesize = original_file[1]
        num_chunks = len(owned_chunks[filename])
        temp.append(filename)
        temp.append(filesize)
        temp.append(num_chunks)
        send_info.append(temp)
    owned_chunks_lock.release()
    send_info.append(udpPort)
    send_info.append(udpIP)
    send_info_byte = pickle.dumps(send_info)
    peerSocket.send(send_info_byte)

def update_alive_fileinfo(modified_fileinfo):
    alive_chunks_lock.acquire()
    filename = modified_fileinfo.filename
    alive_chunks[filename] = modified_fileinfo
    alive_chunks_lock.release()

def update_alive_chunks(modified_chunks):
    filename = modified_chunks[0]
    chunkid = int(modified_chunks[1])
    udpPort = int(modified_chunks[2])
    udpIP = modified_chunks[3]
    alive_chunks_lock.acquire()
    peer = combinePeer(udpPort, udpIP)
    alive_chunks[filename].update_ownerdic(peer, chunkid)
    alive_chunks_lock.release()

def main():
    #get arguments
    tracker_address = sys.argv[1]
    tracker_port = int(sys.argv[2])
    global min_alive_time
    min_alive_time = int(sys.argv[3])

    #setup chunk
    setup_chunks()

    #setup tcp connection and udp connection
    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    peerSocket.connect((tracker_address, tracker_port))

    udpPort = find_open_port(socket.SOCK_DGRAM)
    udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udpSocket.bind(('', udpPort))
    udpIP = socket.gethostbyname(socket.gethostname())
    udpSocket.settimeout(min_alive_time)

    #open threads
    thread_tcp = myThread("tcp", peerSocket, udpSocket, udpPort, udpIP)
    thread_udp_send = myThread("udp_send", peerSocket, udpSocket, udpPort, udpIP)
    thread_udp_receive = myThread("udp_receive", peerSocket, udpSocket, udpPort, udpIP)
    thread_tcp.start()
    thread_udp_send.start()
    thread_udp_receive.start()
    
    threads = []
    threads.append(thread_tcp)
    threads.append(thread_udp_send)
    threads.append(thread_udp_receive)

    for t in threads:
        t.join()
    
    peerSocket.close()
    udpSocket.close()

    numfiles = len(owned_chunks)
    stdout_lock.acquire()
    print("PEER " + str(peer_identifier) + " SHUTDOWN: HAS " + str(numfiles))
    for filename in owned_chunks:
        print(str(peer_identifier) + "    " + filename)
    stdout_lock.release()

if __name__ == "__main__":
    main()