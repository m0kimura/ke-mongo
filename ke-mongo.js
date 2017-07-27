//#######1#########2#########3#########4#########5#########6#########7#########8#########9#########0
const Utility=require('ke-utility');
const Mg=require('mongodb');

module.exports = class Mongo extends Utility {
  constructor(op) {
    this.Db = {}; this.Table = '';
  //ok
    let me=this; op=op||{}; let rc;
    if(!me.CFG){me.info();}
    op.server=op.server||me.CFG.mongo.server; op.port=op.port||me.CFG.mongo.port;
    op.db=op.db||me.CFG.mongo.db;
    promise().resolve().then(()=> {
      let dbn='mongodb://'+op.server+':'+op.port+'/'+op.db;
      Mg.MongoClient.connect(dbn, (err, db)=> {
        if(err){this.error=err; rc=false;}
        else{me.Db=db; me.Table=op.table; rc=true;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
// read
//    (テーブル名, {キー:値}, [項目])
  read(keys, items, op) {
    let me=this; let rc=0; me.REC=[];
    promise().resolve().then(()=> {
      let a, i; let fields={};
      let cur=me.Db.collection(me.Table).find(keys, items, op);
      cur.toArray(function(err, docs){
        if(err){me.error=err; rc=0;}
        else{me.REC=docs; rc=docs.length;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
// insert
//
  insert(op) {
    let me=this;
    let ix, rc;
    for(ix in me.REC){me.ins(ix, op);}
  }
  ins(ix, op) {
    let me = this, rc;
    promise.resolve().then( () => {
      if( me.REC[ix]._id ){delete me.REC[ix]._id;}
      me.Db.collection(me.Table).insert(me.REC, op, function(err, obj){
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then( () => {
      return rc;
    });
  }
//
//
  rewrite() {
    let me=this; var rc=false;
    let ix; for( ix=0; ix<me.REC.length; ix++ ){
      if(!me.rew(ix)) { return false;}
    }
    return true;
  }
  rew(ix) {
    let me = this, ex = 0, rc; ix=ix||0;
    promise().resolve().then(function(){
      if(me.REC[ix]._id){
        me.Db.collection(me.Table).save(me.REC[ix], {w: 1}, (err)=> {
          if( err ) { me.error=err; rc=false; }else{ rc=true; }
          ex++; if( ex >= me.REC.length ){ resolve(); }
        });
      }else{
        me.error="データにIDがありません。ix="+ix;
        rc=false; resolve();
      }
    }).then(function(){
      return rc;
    });
  }
//
  delete() {
    let me=this, rc=false;  op=op||{}; op.multi=op.multi||true;
    let q={};
    let ix; for( ix=0; ix < me.REC.length; ix++) {
      if( ! me.del(ix) ) { return false; }
    }
    return true;
  }
  del(ix) {
    let me=this, ex = 0;
    promise().resolve().then(()=>{
      me.Db.collection( me.Table ).remove( { _id: me.REC[ix]._id }, op, (err) => {
        if( err ){ me.error = err; rc = false; ressolve(); }
        else{ rc = true; }
        ex++; if( ex >= me.REC.length ){ resolve(); }
      });
    }).then(()=>{
      return rc;
    });
  }
//
  get(keys, op) {
    let me=this, rc;
    promise().resolve().then(()=> {
      let a, i; let fields={};
      let cur=me.Db.collection(me.Table).find(keys, '*', op);
      cur.toArray(function(err, docs){
        if(err){me.error=err; rc=false;}
        else{rc=docs;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
  put(rec, op) {
    var me=this; var rc=false; op=op||{}; op.multi=op.multi||true;
    promise().resolve().then(()=> {
      me.Db.collection(me.Table).insert([rec], op, (err, d) =>{
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
  save(rec, op) {
    var me=this; var rc=false; op=op||{};
    promise().resolve().then(()=> {
      me.Db.collection(me.Table).save(rec, op, (err, d) =>{
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
  replace(keys, rec, op) {
    var me=this; var rc=false; op=op||{}; op.multi=op.multi||true;
    promise().resolve().then(()=> {
      me.Db.collection(me.Table).update(keys, rec, op, (err, d)=> {
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then(()=> {
         return rc;
    });
  }
//
  remove(keys, op) {
    var me=this; var rc=false; op=op||{}; op.multi=op.multi||true;
    promise().resolve().then(()=> {
      me.Db.collection(me.Table).remove(keys, op, function(err){
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
  indexed(keys, op) {
    var me=this; var rc; for(var k in keys){keys[k]=1;} op=op||{}; op.w=op.w||1;
    promise().resolve().then(()=> {
      me.Db.collection(me.Table).createIndex(keys, op, function(err){
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then(()=> {
      return rc;
    });
  }
//
  drop() {
    var me=this; var rc;
    promise().resolve().then(()=> {
      me.Db.collection(me.Table).drop(function(err){
        if(err){me.error=err; rc=false;}else{rc=true;}
        resolve();
      });
    }).then(()=> {
        return rc;
    });
  }
};
