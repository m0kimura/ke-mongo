//#######1#########2#########3#########4#########5#########6#########7#########8#########9#########0
const Utility=require('ke-utility');
const Mg=require('mongodb');

module.exports = class Mongo extends Utility {
  constructor(op) {
    super();
    let me=this; op=op||{};
    me.Db = {}; me.Table = '';
    //ok
    if(!me.CFG){me.info();}
    me.CFG.monogo=me.CFG.monogo||{server: 'localhost', port: '27017'};
    op.server=op.server||me.CFG.mongo.server; op.port=op.port||me.CFG.mongo.port;
    op.db=op.db||me.CFG.mongo.db;
    let wid=me.ready();
    let dbn='mongodb://'+op.server+':'+op.port+'/'+op.db;
    let rc;
    Mg.MongoClient.connect(dbn, (err, db)=> {
      if(err){this.error=err; rc=false;}
      else{me.Db=db; me.Table=op.table; rc=true;}
      me.post(wid, rc);
    });
    return me.wait();
  }
  //
  // read
  //    (テーブル名, {キー:値}, [項目])
  read(keys, items, op) {
    let me=this; let rc=0; me.REC=[];
    let wid=me.ready();
    let cur=me.Db.collection(me.Table).find(keys, items, op);
    cur.toArray(function(err, docs){
      if(err){me.error=err; rc=0;}
      else{me.REC=docs; rc=docs.length;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  // insert
  //
  insert(op) {
    let me=this;
    let ix;
    for(ix in me.REC){me.ins(ix, op);}
  }
  ins(ix, op) {
    let me = this, rc;
    let wid=me.ready();
    if( me.REC[ix]._id ){delete me.REC[ix]._id;}
    me.Db.collection(me.Table).insert(me.REC, op, function(err){
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  //
  rewrite() {
    let me=this;
    let ix; for( ix=0; ix<me.REC.length; ix++ ){
      if(!me.rew(ix)) { return false;}
    }
    return true;
  }
  rew(ix) {
    let me = this, ex = 0, rc; ix=ix||0;
    let wid=me.ready();
    if(me.REC[ix]._id){
      me.Db.collection(me.Table).save(me.REC[ix], {w: 1}, (err)=> {
        if( err ) { me.error=err; rc=false; }else{ rc=true; }
        ex++; if( ex >= me.REC.length ){ me.post(wid); }
      });
    }else{
      me.error='データにIDがありません。ix='+ix;
      rc=false;
      me.post(wid);
    }
    me.wait();
    return rc;
  }
  //
  delete(op) {
    let me=this; op=op||{}; op.multi=op.multi||true;
    let ix; for( ix=0; ix < me.REC.length; ix++) {
      if( ! me.del(ix, op) ) { return false; }
    }
    return true;
  }
  del(ix, op) {
    let me=this, ex = 0, rc;
    let wid=me.ready();
    me.Db.collection( me.Table ).remove( { _id: me.REC[ix]._id }, op, (err) => {
      if( err ){ me.error = err; rc = false; me.post(wid); }
      else{ rc = true; }
      ex++; if( ex >= me.REC.length ){ me.post(wid); }
    });
    me.wait();
    return rc;
  }
  //
  get(keys, op) {
    let me=this, rc;
    let wid=me.ready();
    let cur=me.Db.collection(me.Table).find(keys, '*', op);
    cur.toArray(function(err, docs){
      if(err){me.error=err; rc=false;}
      else{rc=docs;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  put(rec, op) {
    var me=this; let rc=false; op=op||{}; op.multi=op.multi||true;
    let wid=me.ready();
    me.Db.collection(me.Table).insert([rec], op, (err) =>{
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  save(rec, op) {
    var me=this, rc; op=op||{};
    let wid=me.ready();
    me.Db.collection(me.Table).save(rec, op, (err) =>{
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  replace(keys, rec, op) {
    let me=this; let rc=false; op=op||{}; op.multi=op.multi||true;
    let wid=me.ready();
    me.Db.collection(me.Table).update(keys, rec, op, (err)=> {
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  remove(keys, op) {
    var me=this; var rc=false; op=op||{}; op.multi=op.multi||true;
    let wid=me.ready();
    me.Db.collection(me.Table).remove(keys, op, function(err){
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  indexed(keys, op) {
    var me=this; var rc; for(var k in keys){keys[k]=1;} op=op||{}; op.w=op.w||1;
    let wid=me.ready();
    me.Db.collection(me.Table).createIndex(keys, op, function(err){
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
  //
  drop() {
    var me=this; var rc;
    let wid=me.ready();
    me.Db.collection(me.Table).drop(function(err){
      if(err){me.error=err; rc=false;}else{rc=true;}
      me.post(wid);
    });
    me.wait();
    return rc;
  }
};
