var udp = require('./index.js');
var socket = udp({
  timeout: 2000, 
  retry: false
});



var buf = new Buffer([0xF1, 0x00, 0x00, 0x00]);

var onResponse = function (err, response, peer, request, opt) {
	if (err) {
		console.log(err);
		return ;
	}
	
	console.log('ip:port %s, response %o', peer.tid, response);
	// socket.destroy();
};

socket.request(buf, {port: 10000, host: '52.87.156.45', debug: true}, onResponse);
// socket.request(buf, {port: 10050, host: '52.87.156.45', debug: true}, onResponse);
socket.request(buf, {port: 10200, host: '52.87.156.45', debug: true}, onResponse);


