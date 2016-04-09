var express = require('express');
var bodyParser = require('body-parser');
var nedb = require('nedb');
var app = express();

var files = new nedb({
	file:'me.json',
	autoload:true,
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.post('/render', function(req, res){
	console.log(req.body);
	res.end(JSON.stringify(req.body));
	files.findOne({ originalname: req.body.id }, function(err, doc){
		if(err) console.log(err);
		if(doc === null){
			// get file
			// store file in db
		}	
		// render file
	});
}); 

app.listen(8081, function(){
	console.log(":D");
});
