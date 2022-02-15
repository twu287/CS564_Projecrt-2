/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  (clockHand++)%numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
  int cnt = 0;
  for (int i = cnt; i <= numBufs; i++)
  {
    if (bufDescTable[clockHand].valid == true)
    {
      if (bufDescTable[clockHand].refbit == true)
      {
        bufDescTable[clockHand].refbit = false;
        advanceClock();
      }
      else if (bufDescTable[clockHand].pinCnt != 0)
      {
        cnt++;
        advanceClock();
      }
      else if (bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty == true)
      {
        flushFile(bufDescTable[clockHand].file);
        frame = clockHand;
        return;
      }
      else if (bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty == false)
      {
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        frame = clockHand;
        return;
      }
    }
    else
    {
      frame = clockHand;
      return;
    }
  }
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {

        //should use hashtable to check?
    bool exist = false;
    int i;
   
    for (i = 0; i < bufPool.size(); i++) {
      if(bufPool[i].page_number() == pageNo) {
        exist = true;
        break;
      }
    }

    if (exist) 
      throw PageNotPinnedException(bufDescTable[i].file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo); // const std::string &nameIn, PageId pageNoIn,FrameId frameNoIn

  
  if (bufDescTable[i].pinCnt > 0 && !dirty ) {
    bufDescTable[i].pinCnt --;
  }

  else if (bufDescTable[i].pinCnt > 0 && dirty) {
    bufDescTable[i].pinCnt --;
    bufDescTable[i].dirty = false;
  }

  else {
    throw PageNotPinnedException(bufDescTable[i].file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);;
  }
    
}

void BufMgr::allocPage(File &file, PageId &pageNo, Page *&page)
{

  FrameId frameNumber;
  allocBuf(frameNumber);
  Page *tempPage = file->allocatePage();
  bufPool[frameNumber] = tempPage;      
  page = &bufPool[frameNumber];
  pageNo = page->page_number();
    
  bufDescTable[frameNumber].Set(file, pageNo);


  hashTable->insert(file, pageNo, frameNumber);
    
}

void BufMgr::flushFile(File& file) {
      
    int i;
  
  for (i = 0; i < bufPool.size(); i++) {
    
    if(bufDescTable[i].file.filename() == file.filename()) {

      if(bufDescTable[i].pinCnt > 0) { //Throws PagePinnedException if some page of the file is pinned.
        throw PagePinnedException(bufDescTable[i].file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
      }

        if(!bufDescTable[i].valid) { //Throws BadBufferException if an invalid page belonging to the file is encountered.
        throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      
      if(bufDescTable[i].dirty) { //(a) if the page is dirty, call file.writePage() to flush the page to disk and then set the dirty bit for the page to false.
        file.writePage(file.readPage(bufDescTable[i].pageNo));
        //file.writePage(bufDescTable[i].pageNo, file.readPage(bufDescTable[i].pageNo));
        bufDescTable[i].dirty = false;
      } 

      hashTable.remove(file, bufDescTable[i].pageNo); //(b) remove the page from the hashtable (whether the page is clean or dirty)

      bufDescTable[i].clear(); //(c) invoke the Clear() method of BufDesc for the page frame.

    }
  }   
}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
