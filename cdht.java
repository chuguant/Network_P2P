// -*- coding: utf-8 -*-
// @File  : cdht.java
// @Author: Chuguan Tian
// @ID    : Z5145006
// @Date  : 2019/4/19

import java.io.*;
import java.net.*;



public class cdht {
	static int identifier;
	int port;
	static int successor_1;
	static int successor_2;
	static int predecessor_1;
	static int predecessor_2;
	static int first_dht = 0;
	static int seq_num_1 = 0;
	static int seq_num_2 = 0;
	static int res_1 = 0;
	static int res_2 = 0;
	static int MSS;
	static float drop_prob;
	DatagramSocket udp_socket;
	ServerSocket tcp_server_socket;
	Socket tcp_socket;
	// int[] responesState = {0,0};

	static int parseInt(String string) {
		return Integer.valueOf(string).intValue();
	}

	static float parseFloat(String string) {
		return Float.valueOf(string).floatValue();
	}

	void sendRequst(int des_iden, DatagramSocket socket,int i) throws IOException, InterruptedException {
		// Create a datagram socket for receiving and sending UDP packets
		int des_port = des_iden+50000;
		InetAddress IPAddress =InetAddress.getLocalHost();
		byte[] Message = new byte[100];
		String msg;
		msg="request "+i;
		Message = msg.getBytes();
		DatagramPacket request =new DatagramPacket(Message,Message.length,IPAddress,des_port );
		socket.send(request);
		Thread.sleep(1000);
	}

	public static void sendTCPMessage(String str,int dest_iden) throws Exception {
		Socket socket = new Socket(InetAddress.getLocalHost(), 50000+dest_iden);
		BufferedWriter socket_writer = new BufferedWriter(new OutputStreamWriter(
				socket.getOutputStream()));
		socket_writer.write(str);
		socket_writer.flush();
		socket.close();
	}

	public static void pingServer(cdht dht) {
		int[] who_is_pre = {-1,-1};
		int i = 0;
		while (true) {
			try {
				DatagramPacket packet_recived = new DatagramPacket(new byte[1024], 1024);
				dht.udp_socket.receive(packet_recived);
				InetAddress client_host = packet_recived.getAddress();
				int client_port = packet_recived.getPort();
				int src_iden = client_port-50000;
				String data_recived = new String(packet_recived.getData(), 0, packet_recived.getLength());
				if (data_recived.split(" ")[0].equalsIgnoreCase("request")) {
					if (i<=1) {
						who_is_pre[i] = packet_recived.getPort()-50000;
						i++;
					}
					if(data_recived.split(" ")[1].equals("0")) 
						System.out.println("A ping request message was received from Peer "+src_iden);
					// Send reply.
					byte[] send_data = ("response "+data_recived.split(" ")[1]).getBytes();
					DatagramPacket reply = new DatagramPacket(send_data, send_data.length, client_host, client_port);
					dht.udp_socket.send(reply);
				}
				else if (data_recived.split(" ")[0].equalsIgnoreCase("response")) {
					if(packet_recived.getPort()-50000==successor_1)
						seq_num_1++;
					if(packet_recived.getPort()-50000==successor_2)
						seq_num_2++;
					if(data_recived.split(" ")[1].equals("0"))
						System.out.println("A ping response message was received from Peer " + (packet_recived.getPort()-50000));
				}
				// decide the predecessors.
				if (who_is_pre[0]!=-1 && who_is_pre[1]!=-1 && i==2) {
					if (who_is_pre[0]>who_is_pre[1]) {
						cdht.predecessor_1 = who_is_pre[0];
						cdht.predecessor_2 = who_is_pre[1];
					}
					else {
						cdht.predecessor_1 = who_is_pre[1];
						cdht.predecessor_2 = who_is_pre[0];
					}
				}
				if(cdht.predecessor_1>cdht.identifier) {
					cdht.first_dht = 1;
				}
			}
			catch (IOException error) {
			}
		}
	}

	int hash(int fileName) {
		return fileName % 256;
	}

	public static void main(String[] args) throws Exception {
		cdht dht = new cdht();
		Thread cur = Thread.currentThread();
		cur.setPriority(5);
		cdht.identifier = parseInt(args[0]);
		cdht.successor_1= parseInt(args[1]);
		cdht.successor_2 = parseInt(args[2]);
		cdht.MSS = parseInt(args[3]);
		cdht.drop_prob = parseFloat(args[4]);
		dht.port = cdht.identifier+50000;
		InetAddress IPAddress =InetAddress.getLocalHost();
		dht.udp_socket = new DatagramSocket(dht.port,IPAddress);

		// start ping client
		Thread send_ping = new Thread() {
			public void run() {
				int[] suc = {cdht.successor_1,cdht.successor_2};
				int[] pre_seq = {0,0};
				while (true) {
					try {
						//if suc_1 is killed
						if(res_1>10) {
							Thread.sleep(1000);
							System.out.println("Peer "+successor_1+" is no longer alive.");
							successor_1 = successor_2;
							Thread.sleep(1000);
							System.out.println("My first successor is now peer "+successor_2+".");
							String str_find_new_suc = "whoIsYourSuc "+identifier;
							Runnable runnable = () -> {
								try {
									res_1 = 0;
									sendTCPMessage(str_find_new_suc, successor_2);
								} catch (Exception error) {
									error.printStackTrace();
								}
							};
							new Thread(runnable).start(); // start a thread of find new successor
						}
						//if suc_2 is killed
						if(res_2>15) {
							String str_find_new_suc = "whoIsYourSuc "+identifier;
							Runnable runnable = () -> {
								try {
									Thread.sleep(1000);
									System.out.println("Peer "+successor_2+" is no longer alive.");
									Thread.sleep(1000);
									System.out.println("My first successor is now peer "+successor_1+".");
									res_2 = 0;
									sendTCPMessage(str_find_new_suc, successor_1);
								} catch (Exception error) {
									error.printStackTrace();
								}
							};
							new Thread(runnable).start(); // start a thread of find new successor
						}
						// send ping msg
						if (cdht.successor_1==suc[0]) {
							dht.sendRequst(cdht.successor_1,dht.udp_socket,seq_num_1);
							if(pre_seq[0]==seq_num_1)
								res_1++;
							else {
								res_1 = 0;
							}
							pre_seq[0]=seq_num_1;
							suc[0]=cdht.successor_1;
						}
						else {
							seq_num_1=0;
							dht.sendRequst(cdht.successor_1,dht.udp_socket,seq_num_1);
							pre_seq[0]=seq_num_1;
							suc[0]=cdht.successor_1;
						}
						if(cdht.successor_2==suc[1]) {
							dht.sendRequst(cdht.successor_2,dht.udp_socket,seq_num_2);
							if(pre_seq[1]==seq_num_2)
								res_2++;
							else {
								res_2 = 0;
							}
							pre_seq[1]=seq_num_2;
							suc[1]=cdht.successor_2;
						}
						else {
							seq_num_2=0;
							dht.sendRequst(cdht.successor_2,dht.udp_socket,seq_num_2);
							pre_seq[1]=seq_num_2;
							suc[1]=cdht.successor_2;	
						}
					}catch (InterruptedException error) {
						error.printStackTrace();
					} catch (IOException error) {
						error.printStackTrace();
					}
					try {
						Thread.sleep(300);
					} catch (InterruptedException error) {
						error.printStackTrace();
					}
				}
			}
		};
		send_ping.setPriority(2);
		send_ping.start();


		// start pingServer
		Runnable pingServer = () -> pingServer(dht);
		new Thread(pingServer).start(); 

		// start TCP Server
		Thread tcpServer =new Thread(){
			public void run() {
				try {
					dht.tcp_server_socket = new ServerSocket(dht.port);
					while(true) {
						Socket tcp_socket = dht.tcp_server_socket.accept();
						BufferedReader buf = new BufferedReader(new InputStreamReader(tcp_socket.getInputStream())); 
						String input = buf.readLine();		
						if(input.split(" ")[0].equals("requestFile")) {
							// input should be "requestFile sourcePortIden fileName".
							//start request file procession
							int fileName_from_request = parseInt(input.split(" ")[2]);
							if(fileName_from_request == cdht.identifier 
									|| (fileName_from_request>cdht.predecessor_1 && fileName_from_request<cdht.identifier) 
									|| (cdht.first_dht==1 && cdht.predecessor_1<fileName_from_request)){
								// start a new thread of send file.
								//Send file to source.
								//fileResponse should be "fileResponse storedIden fileName"
								String fileResponse = "fileResponse "+cdht.identifier+" "+input.split(" ")[2];	
								Runnable sendFile = () -> {
									try {
										sendTCPMessage(fileResponse,parseInt(input.split(" ")[1]));
										Thread.sleep(1000);
										System.out.println("File "+input.split(" ")[2]+" is here.");
										Thread.sleep(1000);
										System.out.println("A response msg, destined for peer "+input.split(" ")[1]+", has been sent.");
										Thread.sleep(1000);
										System.out.println("We now start sending the file .........");
										Thread.sleep(3000);
										System.out.println("The file is sent.");
									} catch (IOException error) {
										error.printStackTrace();
									} catch (Exception error) {
										error.printStackTrace();
									}
								};
								new Thread(sendFile).start(); 
							}
							else {
								//Pass request to successor.
								// start a new thread of pass request.
								Runnable requestFile = () -> {
									try {
										sendTCPMessage(input, cdht.successor_1);
										Thread.sleep(1000);
										System.out.println("File "+input.split(" ")[2]+" is not stored here.");
										Thread.sleep(1000);
										System.out.println("File request message has been forwarded to my successor.");
									} catch (IOException error) {
										error.printStackTrace();
									} catch (Exception error) {
										error.printStackTrace();
									}
								};
								new Thread(requestFile).start(); 
							}
						}
						else if (input.split(" ")[0].equals("quit")) {
							//start quit procession
							// quit msg should be "quit quitIden suc_1 suc_2 preNum"
							if(input.split(" ")[4].equals("1")) {
								cdht.successor_1 = parseInt(input.split(" ")[2]);
								cdht.successor_2 = parseInt(input.split(" ")[3]);
								System.out.println("Peer "+input.split(" ")[1]+" will depart from the network.");
								System.out.println("My first successor is now peer "+cdht.successor_1+".");
								System.out.println("My second successor is now peer "+cdht.successor_2+".");
							}
							else {
								cdht.successor_2 = parseInt(input.split(" ")[2]);
								System.out.println("Peer "+input.split(" ")[1]+" will depart from the network.");
								System.out.println("My first successor is now peer "+cdht.successor_1+".");
								System.out.println("My second successor is now peer "+cdht.successor_2+".");
							}
						}
						else if (input.split(" ")[0].equals("whoIsYourSuc")) {
							Runnable requestFile = () -> {
								try {
									String my_suc = "mySucIs "+successor_1;
									sendTCPMessage(my_suc, parseInt(input.split(" ")[1]));
								} catch (IOException error) {
									error.printStackTrace();
								} catch (Exception error) {
									error.printStackTrace();
								}
							};
							new Thread(requestFile).start(); 
						}
						else if (input.split(" ")[0].equals("mySucIs")) {
							successor_2 = parseInt(input.split(" ")[1]);
							System.out.println("My second successor is now peer "+successor_2+".");
							seq_num_2 = 0;
						}
						else if (input.split(" ")[0].equals("fileResponse")) {
							try {
								Thread.sleep(1000);
								System.out.println("Received a response msg from peer "+input.split(" ")[1]+", which has the file "+input.split(" ")[2]+".");
								Thread.sleep(1000);
								System.out.println("We now start receiving the file .........");
								Thread.sleep(5000);
								System.out.println("The file is received.");
							} catch (Exception error) {
								error.printStackTrace();
							}
						}

					}
				}
				catch (IOException error) {
					error.printStackTrace();
				}
			}
		};
		tcpServer.start();

		// Listen to the console
		while (true) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String order = br.readLine();

			//REQUESTING A FILE
			if (order.indexOf("request")!=-1) {
				String fileName = order.split(" ")[1];

				// start a new thread of start request.
				Runnable start_request_file = () -> {
					try {
						String start_data = "requestFile "+cdht.identifier + " " +fileName;
						sendTCPMessage(start_data, cdht.successor_1);
						Thread.sleep(1000);
						System.out.println("File request message for "+order.split(" ")[1]+" has been sent to my successor.");
					} catch (IOException error) {
						error.printStackTrace();
					} catch (Exception error) {
						error.printStackTrace();
					}
				};
				new Thread(start_request_file).start(); 
			}
			else if (order.indexOf("quit")!=-1) {
				// start quit procession
				// quit msg should be "quit quitIden suc_1 suc_2 preNum"
				String quit_Msg1 = "quit "+cdht.identifier+" "+cdht.successor_1+" "+cdht.successor_2+" "+1;
				sendTCPMessage(quit_Msg1, cdht.predecessor_1);
				String quit_Msg2 = "quit "+cdht.identifier+" "+cdht.successor_1+" "+cdht.successor_2+" "+2;
				sendTCPMessage(quit_Msg2, cdht.predecessor_2);
			}

		}//end of loop
	}
}

//reference:
//https://stackoverflow.com/questions/6380057/python-binding-socket-address-already-in-use
//http://www.linuxcommand.org/lc3_lts0100.php
// https://blog.csdn.net/omnispace/article/details/80195259
// https://blog.csdn.net/JAVA_HHHH/article/details/79778396
// https://blog.csdn.net/chief1985/article/details/2222856
// https://blog.csdn.net/qq_40866897/article/details/82958395