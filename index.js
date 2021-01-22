const { Readable,Writable } = require("stream");
// const inStream = new Readable();

// inStream.push("ABCDEFGHIJKLM");
// inStream.push("NOPQRSTUVWXYZ");

// inStream.push(null); // No more data

// inStream.pipe(process.stdout);




const inStream = new Readable({
    read(size) {
      let a = String.fromCharCode(this.currentCharCode++);
      this.push(a);
      console.log('---->sz = ',size,'--->',a)
      if (this.currentCharCode > 128) {
        this.push(null); //end stream
      }
    }
  });
  
  inStream.currentCharCode = 65;
  
  inStream.pipe(process.stdout);