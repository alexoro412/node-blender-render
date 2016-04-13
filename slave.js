const express = require('express');
const bodyParser = require('body-parser');
const nedb = require('nedb');
const http = require('http');
const exec = require('child_process').exec;
const fs = require('fs');
const app = express();
const request = require('request');
const zpad = require('zpad');
const needle = require('needle');


var this_port = 8081;
var this_address = "127.0.0.1";
var this_id = "slave1";

var masterurl = "127.0.0.1:8080/register_slave";

var files = new nedb({
	file:'./me.json',
	autoload:true,
});

app.use(express.static('files'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// make a register post request

app.post('/render', function(req, res){
	console.log(req.body);
	res.end(JSON.stringify(req.body));
	files.findOne({ id: req.body.id }, function(err, doc){
		if(err) console.log(err);
		if(doc === null){
			// get file
			var file = fs.createWriteStream(req.body.id);
			var request = http.get("http://127.0.0.1:8080/get_file?filename=" + req.body.id,
				function(response){
					response.pipe(file);
				  response.on('end', () => {
						file_entry = {
							id: req.body.id,
							path: file.path,
						};
						files.insert(file_entry, function(err, newDoc){
							render(req.body.id, req.body.frame);
						}); 
					});
				});
			// store file in db
		}else{
			render(req.body.id, req.body.frame);
		}
		// render file
	});
}); 

function render(id, frame){
	files.findOne({id:id}, function(err, doc){
		var cmd = 'blender --background -F PNG ./' + doc.path + ' -o ./' + doc.id + '_#### -x 1 -F PNG -f ' + frame + ' -F PNG';
		console.log(cmd);
		var process = exec(cmd, function(err, stdout, stderr){
			if(err) console.log(err);
		});
		process.on('close', (code) => {
			console.log("finished render");
			// POST finished
			var req = request.post('http://localhost:8080/finished', function (err, resp, body) {
  			if (err) {
  			  console.log(err);
			  } else {
 			   console.log('URL: ' + body);
				}
			});

			var form = req.form();
			form.append('file', fs.createReadStream(doc.id + '_' + zpad(frame, 4) + '.png'));
			form.append('id', this_id);
			form.append('job_id', id);
			form.append('frame', frame);
			req.on('end', () => {console.log("D")});
		});
	});
}

app.listen(this_port, function(){
	console.log(":D");

	needle.post(masterurl, {id:this_id, address:this_address, port:this_port}, {}, function(err,resp){if(err) console.log(err);});
});
