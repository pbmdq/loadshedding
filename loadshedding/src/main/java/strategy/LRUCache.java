package strategy;

/*
 * XAPool: Open Source XA JDBC Pool
 * Copyright (C) 2003 Objectweb.org
 * Initial Developer: Lutris Technologies Inc.
 * Contact: xapool-public@lists.debian-sf.objectweb.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 */

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Enumeration;

import data_entry.DataEntry;

/**
 * Simple implementation of a cache, using Least Recently Used algorithm
 * for discarding members when the cache fills up
 */
public class LRUCache
{
    class CacheNode
    {

        CacheNode prev;
        CacheNode next;
        DataEntry value;
        Object key;
        
    }


    public LRUCache(int i) {
        currentSize = 0;
        cacheSize = i;
        nodes = new Hashtable(i);
    }

    public DataEntry get(Object key) {
        CacheNode node = (CacheNode)nodes.get(key);
        if(node != null) {
            moveToHead(node);
            return node.value;
        }
        else{
            return null;
        }
    }

    public Object put(Object key, DataEntry value) {
        CacheNode node = (CacheNode)nodes.get(key);
        DataEntry remove = null;
        if(node == null)
        {
            if(currentSize >= cacheSize)
            {
                if(last != null) {
                	CacheNode temp = (CacheNode) nodes.get(last.key);
                	remove = temp.value;
                	nodes.remove(last.key);
                }
                removeLast();
            }
            else
            {
                currentSize++;
            }
            node = new CacheNode();
        }
        node.value = value;
        node.key = key;
        moveToHead(node);
        nodes.put(key, node);
        return remove;
    }

    public DataEntry remove(Object key) {
        CacheNode node = (CacheNode)nodes.get(key);
        if (node != null) {
            if (node.prev != null) {
                node.prev.next = node.next;
            }
            if (node.next != null) {
                node.next.prev = node.prev;
            }
            if (last == node)
                last = node.prev;
            if (first == node)
                first = node.next;
            
            currentSize--;
        }
        
        return node.value;
    }

    public void clear()
    {
        first = null;
        last = null;
    }

    private void removeLast()
    {
        if(last != null)
        {
            if(last.prev != null)
                last.prev.next = null;
            else
                first = null;
            last = last.prev;
        }
    }
    public DataEntry removeLastEntry() {
        if(last != null)
        {
        	CacheNode temp = last;
            if(last.prev != null)
                last.prev.next = null;
            else
                first = null;
            last = last.prev;
            currentSize--;
            return temp.value;
        }
        return null;
        
    }

    private void moveToHead(CacheNode node)
    {
        if(node == first)
            return;
        if(node.prev != null)
            node.prev.next = node.next;
        if(node.next != null)
            node.next.prev = node.prev;
        if(last == node)
            last = node.prev;
        if(first != null)
        {
            node.next = first;
            first.prev = node;
        }
        first = node;
        node.prev = null;
        if(last == null)
            last = first;
    }
    public int getSize() {
    	return currentSize;
    }
    private int cacheSize;
    private Hashtable nodes;
    private int currentSize;
    private CacheNode first;
    private CacheNode last;
}