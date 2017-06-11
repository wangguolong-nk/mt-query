# -*- coding:utf-8 -*-
import numpy as np
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
import sys
from sklearn import preprocessing
import random
import pickle
import os

mode=1
datadir='/Users/zj-db0803/wgl_work/meitu_data/quality_train/train'
modeldir='/Users/zj-db0803/wgl_work/meitu_data/quality_train/model'
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    if argk=='-mode':
        mode=int(argv)
    elif argk=='-datadir':
        datadir=argv
    elif argk=='-modeldir':
        modeldir=argv
if mode==1:#StandardScaler
    suffix = '.stand'
    scaler=preprocessing.StandardScaler()
elif mode==2:#MinMaxScaler
    suffix='.minmax'
    scaler = preprocessing.MinMaxScaler()
else:
    sys.exit(1)
negf=open(datadir+'/vect.neg','w+')
posf=open(datadir+'/vect.pos','w+')
for root,dirs,filenames in os.walk(datadir):
    for filename in filenames:
        print filename
        hflag=False
        lfalg=False
        if filename.endswith('.vect.high.train'):
            hflag=True
        elif filename.endswith('.vect.low.train'):
            lfalg=True
        else:
            continue
        with open(root+'/'+filename) as fi:
            for line in fi.readlines():
                fields=line.strip().split(',')
                if hflag:#14
                    posf.write(','.join(fields[1:7])+',1'+'\n')
                elif lfalg:
                    negf.write(','.join(fields[1:7])+',0'+'\n')
posf.close()
negf.close()

negf=open(datadir+'/vect.neg')
posf=open(datadir+'/vect.pos')
dataset_neg=np.loadtxt(negf,delimiter=',')
dataset_pos=np.loadtxt(posf,delimiter=',')
negf.close()
posf.close()

neg_size=dataset_neg.shape[0]
pos_size=dataset_pos.shape[0]
print dataset_neg.shape,neg_size
print dataset_pos.shape,pos_size
real_neg_size=pos_size*9
indexset=set()
while len(indexset)<real_neg_size:
    r = random.randint(1,neg_size-1)
    indexset.add(r)
new_neg_dataset=[]
for rindex in indexset:
    rowdata=dataset_neg[rindex,:]
    new_neg_dataset.append(rowdata)
new_neg_dataset=np.array(new_neg_dataset)

dataset_neg=new_neg_dataset

test_pos_wc=pos_size/10
test_neg_wc= test_pos_wc * 4
train_negX= dataset_neg[test_neg_wc:, :-1]#6个特征
train_negY= dataset_neg[test_neg_wc:, -1]
train_posX= dataset_pos[test_pos_wc:, :-1]
train_posY= dataset_pos[test_pos_wc:, -1]

test_negX= dataset_neg[:test_neg_wc, :-1]
test_negY= dataset_neg[:test_neg_wc, -1]
test_posX= dataset_pos[:test_pos_wc, :-1]
test_posY= dataset_pos[:test_pos_wc, -1]
print 'train_negX',train_negX
print 'train_posX',train_posX

X=np.vstack((train_posX,train_negX))
Y=np.hstack((train_posY,train_negY))
print 'X.shape=',X.shape
print 'Y.shape=',Y.shape
testX=np.vstack((test_posX,test_negX))
testY=np.hstack((test_posY,test_negY))
print 'testX.shape=',testX.shape
print 'testY.shape=',testY.shape

scaler=scaler.fit(X)
with open(modeldir+'/model.scaler.pickle'+suffix,'wb+') as f:
    pickle.dump(scaler,f)
print 'scaler.std_=',scaler.std_,type(scaler.std_)
print 'scaler.mean_=',scaler.mean_,type(scaler.mean_)
X_minmax=scaler.transform(X)
testX_minmax=scaler.transform(testX)
print 'X_train_scaler over'
X=X_minmax

# model=LogisticRegression(C=0.2,penalty='l2')
model=LogisticRegression()
# model=LogisticRegression(C=1e5,penalty='l2')
# model=LogisticRegression()
model.fit(X,Y)
print 'coef_',model.coef_
real_testX=testX_minmax
real_testY=testY
predicted=model.predict_proba(real_testX)
answer=predicted[:,1]
report=answer>0.5
roc_auc_score=metrics.roc_auc_score(real_testY,answer)
print 'roc_auc_score=',roc_auc_score
print metrics.classification_report(real_testY,report,target_names=['neg','pos'])
print modeldir+'/model.lr.pickle'+suffix
with open(modeldir+'/model.lr.pickle'+suffix,'wb+') as f:
    pickle.dump(model,f)