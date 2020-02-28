from socket import *
from fileinfo import fileinfo
import sys
import threading
import pickle
import collections

#set up variables
fileinfos = {} #key: filename, value: fileinfo class
counter = 0
peer_sockets = []
original_files = {} #key: identifier, value: filename
threads = []

counter_lock = threading.Lock()
stdout_lock = threading.Lock()
fileinfo_lock = threading.Lock()
peer_socket_lock = threading.Lock()

class myThread(threading.Thread):
    def __init__(self, peerSocket):
        threading.Thread.__init__(self)
        self.peerSocket = peerSocket
    def run(self):
        peer_connect(self.peerSocket)

def find_open_port(sock):
    s = socket(AF_INET, sock)
    s.bind(('', 0))
    port = s.getsockname()[1]
    return port

def combinePeer(udpPort, udpIP):
    return str(udpPort) + " " + udpIP

def disconnect(updates):
    udpPort = int(updates[1])
    udpIP = updates[2]

    fileinfo_lock.acquire()
    for filename in fileinfos:
        peer = combinePeer(udpPort, udpIP)
        (fileinfos[filename]).delete_peer(peer)
    fileinfo_lock.release()

def find_current_owned_files(peer):
    result = []
    fileinfo_lock.acquire()
    for fileinfo_temp in fileinfos.values():
        ownerdic = fileinfo_temp.ownerdic
        if peer in ownerdic:
            result.append(fileinfo_temp.filename)
    fileinfo_lock.release()
    return result

def peer_connect(peerSocket):
    #assign identifying integer
    counter_lock.acquire()
    global counter
    identifier = counter
    counter += 1
    counter_lock.release()

    #send identifying integer and current alive chunks to peer
    fileinfo_lock.acquire()
    send_info = [identifier, fileinfos]
    fileinfo_lock.release()
    send_info_byte = pickle.dumps(send_info)
    peerSocket.send(send_info_byte)

    #receive initial files/chunks info from peer
    modified_chunks = receive_initial_chunk_info(peerSocket, identifier)
    broadcast(modified_chunks ,"fileinfo")

    #receive updates from peer and then broadcast
    while True:
        #wait for the peer to send updates, send broadcast the updates to all peers
        #break if the peer disconnect, the thread terminates
        updates_byte = peerSocket.recv(1024)
        try:
            updates = pickle.loads(updates_byte)
        except:
            continue
        if updates[0] == "disconnect":
            updates.append(identifier)
            broadcast(updates, "disconnect")

            #print information(refer to guide)
            original_file = original_files[identifier]
            port = int(updates[1])
            ip = updates[2]
            peer = combinePeer(port, ip)
            owned_files_now = find_current_owned_files(peer) #list of filename
            numfiles = len(owned_files_now)
            stdout_lock.acquire()
            print("PEER " + str(identifier) + " DISCONNECT: RECEIVED " + str(numfiles))
            for filename in owned_files_now:
                print(str(identifier) + "    " + filename)
            stdout_lock.release()
            
            disconnect(updates)
            global threads
            threads.pop()
            peer_socket_lock.acquire()
            peer_sockets.remove(peerSocket)
            peer_socket_lock.release()
            peerSocket.close()
            break

        broadcast(updates, "update")

        filename = updates[0]
        chunkid = updates[1]
        owner = combinePeer(int(updates[2]), updates[3])

        fileinfo_lock.acquire()
        fileinfos[filename].update_ownerdic(owner, int(chunkid))
        numchunks = fileinfos[filename].numchunks
        owned_chunks = fileinfos[filename].count_chunk(owner)
        fileinfo_lock.release()

def receive_initial_chunk_info(peerSocket, identifier):
    receive_info_byte = peerSocket.recv(1024)
    receive_info = pickle.loads(receive_info_byte)

    num_files = len(receive_info) - 2
    udpPort = receive_info[num_files]
    udpIP = receive_info[num_files+1]
    filenames = []
    filesizes = []
    numchunks = []
    for x in range(num_files):
        fileinfo_from_peer = receive_info[x]
        filenames.append(fileinfo_from_peer[0])
        filesizes.append(fileinfo_from_peer[1])
        numchunks.append(fileinfo_from_peer[2])
    
    #write to stdout
    stdout_lock.acquire()
    print("PEER " + str(identifier) + " CONNECT: OFFERS " + str(num_files))
    for x in range(int(num_files)):
        print(str(identifier) + "    " + filenames[x] + " " + str(numchunks[x]))
        original_files[identifier] = filenames[x]
    stdout_lock.release()

    #write to fileinfos
    temp_fileinfo = None
    fileinfo_lock.acquire()
    for x in range(int(num_files)):
        if filenames[x] not in fileinfos:
            ownerdic = {}
            peer = combinePeer(udpPort, udpIP)
            ownerdic[peer] = [[0, numchunks[x]-1]]
            temp_fileinfo = fileinfo.create_fileinfo(filenames[x], filesizes[x], numchunks[x], ownerdic)
            fileinfos[filenames[x]] = temp_fileinfo
    fileinfo_lock.release()

    return temp_fileinfo

def broadcast(modified_chunks, broad_type):
    peer_socket_lock.acquire()
    for peerSocket in peer_sockets:
        info_to_send = [modified_chunks, broad_type]
        info_to_send_byte = pickle.dumps(info_to_send)
        peerSocket.send(info_to_send_byte)
    peer_socket_lock.release()

def main():
    #setup port, and wirte into port.txt
    tracker_port = find_open_port(SOCK_STREAM)
    # print("TRACKER_PORT=" + str(tracker_port))
    file_port = open("port.txt", "w")
    file_port.write(str(tracker_port))
    file_port.close()

    #setup tcp connection
    trackerSocket = socket(AF_INET, SOCK_STREAM)
    trackerSocket.bind(('', tracker_port))
    trackerSocket.listen(8)
    while True:
        peerSocket, address = trackerSocket.accept()
        peer_socket_lock.acquire()
        peer_sockets.append(peerSocket)
        peer_socket_lock.release()
        thread_peer = myThread(peerSocket)
        thread_peer.start()
        threads.append(thread_peer)

    
    trackerSocket.close()

if __name__ == "__main__":
    main()