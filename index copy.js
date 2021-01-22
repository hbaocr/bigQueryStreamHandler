
const json = require('big-json');
const { Readable } = require('stream');
function bufferToStream(binary) {
    const readableInstanceStream = new Readable({
      read() {
        this.push(binary);
        this.push(null);
      }
    });

    return readableInstanceStream;
}


async function parseJson(str){
    return new Promise(function(resolve,reject){
        let buf = Buffer.from(str,'utf8');
        const readStream = bufferToStream(buf);
        const parseStream = json.createParseStream();
        readStream.pipe(parseStream);
        parseStream.on('data', function(jobj) {
            resolve(jobj)
        });
        
    })
}


module.exports={parseJson}
// let str = fs.readFileSync('LogES.txt','utf8');
// parseJson(str).then(json_data=>{
//     console.log(json_data)
// })