const { Readable,Writable } = require("stream");



class StreamUtil{
    constructor(){

    }

    createStreamSrc(){
         this.inStream = new Readable({
            read(size) {
              feedData
              let a = String.fromCharCode(this.currentCharCode++);
              this.push(a);
              console.log('---->sz = ',size,'--->',a)
              if (this.currentCharCode > 128) {
                this.push(null);
              }
            }
          });
    }


}
function


const inStream = new Readable({
    read(size) {
      let a = String.fromCharCode(this.currentCharCode++);
      this.push(a);
      console.log('---->sz = ',size,'--->',a)
      if (this.currentCharCode > 128) {
        this.push(null);
      }
    }
  });
  
  inStream.currentCharCode = 65;
  
  inStream.pipe(process.stdout);