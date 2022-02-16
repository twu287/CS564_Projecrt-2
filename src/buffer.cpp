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

/**
 * @brief Advance clock to next frame in the buffer pool.
 *
 */
void BufMgr::advanceClock() {
  clockHand = (clockHand+1);
  // make it circlular
  clockHand = clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
  int cnt = 0;
  for (int i = cnt; i <= numBufs; i++)
  {
    if (bufDescTable[clockHand].valid)
    {
      if (bufDescTable[clockHand].refbit)
      {
        bufDescTable[clockHand].refbit = false;
        advanceClock();
      }
      else if (bufDescTable[clockHand].pinCnt != 0)
      {
        cnt++;
        advanceClock();
      }
      else if (bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty)
      {
        flushFile(bufDescTable[clockHand].file);
        frame = clockHand;
        return;
      }
      else if (bufDescTable[clockHand].pinCnt == 0 && !bufDescTable[clockHand].dirty)
      {
        hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
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
  throw BufferExceededException();
}
/**
 * @brief the page and need to check if the page is existed in buffer pool already, if yes, just update the pinCount and refbit
 * if not, allocate a new frame, save it in buffer pool and update the hashtable
 *
 * @param file   	File object
 * @param PageNo    Page number
 * @param page  	page object need to return the page pointer to the place where page saved in buffer pointer
 */
void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId id;  
  try{
    // look up the page is existed in buffer pool or not
    hashTable.lookup(file, pageNo, id);
    // exist in buffer pool, increment pinCnt and set refbit true  
    bufDescTable[id].pinCnt++;
    bufDescTable[id].refbit = true;
    //return page pointer to the page in buffer pool  
    page = &bufPool[id];
  }
  // if the page isn't existed in buffe pool, allocate it with the new frame
  catch (HashNotFoundException hnfe){
    //get the new frame  
    allocBuf(id);
    Page newPage = file.readPage(pageNo);
    //add it in buffer pool  
    bufPool[id] = newPage;
    //update the hashtable
    hashTable.insert(file, pageNo, id);
    bufDescTable[id].Set(file, pageNo);
    //return page pointer to the page in buffer pool  
    page = &bufPool[id];
  }
}

/**
 * @brief Find the matched page and uppin it (decrement pin count)
 *
 * @param file    File object
 * @param pageNo  Page no to be searched
 * @param dirty   a bool to define the if it is a dirty page or not 
 * 
 * @throws PageNotPinnedException when a page which is expected to be pinned in the buffer pool is found to be not pinned
 * @throws HashNotFoundException when the page is not found
 */
void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
FrameId id;
    //search the page by pageNo
    try{
        hashTable.lookup(file, pageNo, id);
        //if pinCnt is 0, throw exception
        if (bufDescTable[id].pinCnt == 0){
            throw PageNotPinnedException(file.filename(), pageNo, id);
        }
        bufDescTable[id].pinCnt--;
        //if it's a dirty page, make dirty to be true
        if (dirty) {
          bufDescTable[id].dirty = true;
        }
    }
    catch (HashNotFoundException e){
    }
}


void BufMgr::allocPage(File &file, PageId &pageNo, Page *&page)
{

  FrameId frameNumber;
  allocBuf(frameNumber);
  Page tempPage = file.allocatePage();
  bufPool[frameNumber] = tempPage;      
  page = &bufPool[frameNumber];
  pageNo = page->page_number();
    
  bufDescTable[frameNumber].Set(file, pageNo);


  hashTable.insert(file, pageNo, frameNumber);
    
}
/**
 * @brief Scan bufTable for pages belonging to the file, and clear them from bulpool.
 *
 * @param file    File object
 * 
 * @throws PagePinnedException if some page of the file is pinned.
 * @throws BadBufferException if an invalid page belonging to the file is encountered.
 */
void BufMgr::flushFile(File& file) {
      
    int i;
  //search if the pages are in the bulPool
  for (i = 0; i < bufPool.size(); i++) { 

    if(bufDescTable[i].file.filename() == file.filename()) {
      // if pinCnt of page not equal to 0, can't flush it, throw exception
      if(bufDescTable[i].pinCnt > 0) { 
        throw PagePinnedException(bufDescTable[i].file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
      }
        //if page is not valid, throw exception
        if(!bufDescTable[i].valid) { 
        throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }

      //if the page is dirty, flush the page to disk
      if(bufDescTable[i].dirty) { 
        file.writePage(bufPool[i]);
        bufDescTable[i].dirty = false;
      } 

      //remove the page from the hashtable
      hashTable.remove(file, bufDescTable[i].pageNo);

      //clear it from BuDesc
      bufDescTable[i].clear();

    }
  }   
}
/**
 * @brief the page from buffer pool and remove it from file
 *
 * @param file    File object
 * @param PageNo  Page number
 */
void BufMgr::disposePage(File& file, const PageId PageNo) {
  FrameId id;
  try{
    // look up the page is existed in buffer pool or not  
    hashTable.lookup(file, PageNo, id);  
    bufDescTable[id].clear();
    hashTable.remove(file, PageNo);  
    file.deletePage(PageNo);
  }
  //all pages are free and cleaned  
  catch (HashNotFoundException hnfe){
    printf("deleted page is not existed");
  }
}

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
