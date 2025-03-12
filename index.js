import express from 'express';
import {createServer} from 'node:http';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';
import {Server} from 'socket.io';
import sqlite3 from 'sqlite3'; //importing database 
import {open} from 'sqlite';

//because the await is not wroking , wrapping this in async function
//async function startServer(){
//open database file
const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
});
//}


//create 'messages' tablea
await db.exec(`
    CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
    );
    `);

const app = express();
const server = createServer(app);
const io = new Server(server,{
    connectionStateRecovery: {}
});

// app.get('/', (req ,res) => {
//     res.send("<h1>hello</h1>");
// });

const __dirname = dirname(fileURLToPath(import.meta.url));

app.get('/',(req ,res) => {
    res.sendFile(join(__dirname, 'index.html'));
});

io.on('connection', async(socket) => {
    // console.log('a user connected');
    // socket.on('dissconnect', ()=>{
    //     console.log('user diconnected')
    // })

    socket.on('chat message', async(msg,clientOffSet, callback)=>{ //what is async?
        let result ;
        try{
            //store the messages in the database 
            result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?)',msg,clientOffSet);
        } catch (e){
            //return; //handle failure
            if (e.errno === 19){ //sqlite constraint
                //message already inserted
                callback({
                    status: 'ok'
                });
            }else{
                //do nothing, let client retry
            }
            return;
        }

        console.log("message :" + msg);
        io.emit('chat message', msg ,result.lastID); //row id of the inserted row
        callback({
            status: 'ok'
        });
        //console.log(msg); prints in server terminal
    });
    if (!socket.recovered){
        //if the connection state recovery was not successfull
        try{
            await db.each('SELECT id, content FROM messages WHERE id > ?',
                [socket.handshake.auth.serverOffSet || 0],
                (_err, row) =>{
                    socket.emit('chat messages', row.content , row.id);
                }
            )
        }catch(e){
            console.log(e);
        }
    }

});

//emit the event to all the connected sockets 
//io.emit('hello','world');

server.listen(3000, ()=> {
    console.log("Sever Running on port 3000");
});



/* Room For socket
io.on('connection', (socket) => {
  // join the room named 'some room'
  socket.join('some room');
  
  // broadcast to all connected clients in the room
  io.to('some room').emit('hello', 'world');

  // broadcast to all connected clients except those in the room
  io.except('some room').emit('hello', 'world');

  // leave the room
  socket.leave('some room');
});

*/
