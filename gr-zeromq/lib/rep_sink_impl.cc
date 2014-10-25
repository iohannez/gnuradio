/* -*- c++ -*- */
/*
 * Copyright 2013,2014 Free Software Foundation, Inc.
 *
 * This file is part of GNU Radio.
 *
 * This is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this software; see the file COPYING.  If not, write to
 * the Free Software Foundation, Inc., 51 Franklin Street,
 * Boston, MA 02110-1301, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gnuradio/io_signature.h>
#include "rep_sink_impl.h"

namespace gr {
  namespace zeromq {

    rep_sink::sptr
    rep_sink::make(size_t itemsize, size_t vlen, char *address, int timeout)
    {
      return gnuradio::get_initial_sptr
        (new rep_sink_impl(itemsize, vlen, address, timeout));
    }

    rep_sink_impl::rep_sink_impl(size_t itemsize, size_t vlen, char *address, int timeout)
      : gr::sync_block("rep_sink",
                       gr::io_signature::make(1, 1, itemsize * vlen),
                       gr::io_signature::make(0, 0, 0)),
        d_itemsize(itemsize), d_vlen(vlen), d_timeout(timeout)
    {
      int major, minor, patch;
      zmq::version (&major, &minor, &patch);
      if (major < 3) {
        d_timeout = timeout*1000;
      }
      d_context = new zmq::context_t(1);
      d_socket = new zmq::socket_t(*d_context, ZMQ_REP);
      int time = 0;
      d_socket->setsockopt(ZMQ_LINGER, &time, sizeof(time));
      d_socket->bind (address);
    }

    rep_sink_impl::~rep_sink_impl()
    {
      d_socket->close();
      delete d_socket;
      delete d_context;
    }

    std::string
    rep_sink_impl::serialize_tags(size_t msg_items)
    {
      const uint64_t nread = this->nitems_read(0);
      std::vector<tag_t> tags;
      this->get_tags_in_range(tags, 0, nread, nread+msg_items);
      if (tags.size() == 0) {
        return std::string();
      }
      else {
        pmt::pmt_t tag_list;
        pmt::pmt_t tag_tuple;

        for(std::vector<tag_t>::iterator tags_itr = tags.begin(); tags_itr != tags.end(); tags_itr++) {
          uint64_t rel_offset = tags_itr->offset - nread;
          tag_tuple = pmt::make_tuple(pmt::from_uint64(rel_offset), tags_itr->key, tags_itr->value, tags_itr->srcid);
          pmt::list_add(tag_list,tag_tuple);
        }
        return pmt::serialize_str(tag_list);
      }
    }

    int
    rep_sink_impl::work(int noutput_items,
                        gr_vector_const_void_star &input_items,
                        gr_vector_void_star &output_items)
    {
      const char *in = (const char *) input_items[0];

      zmq::pollitem_t items[] = { { *d_socket, 0, ZMQ_POLLIN, 0 } };
      zmq::poll (&items[0], 1, d_timeout);

      //  if we got a message, process
      if (items[0].revents & ZMQ_POLLIN) {
        // receive data request
        zmq::message_t request;
        d_socket->recv(&request);
        int req_output_items = *(static_cast<int*>(request.data()));

        // reply
        size_t msg_items = 0;
        // determine number of items to put in message
        if (noutput_items < req_output_items) {
          msg_items = noutput_items;
        }
        else {
          msg_items = req_output_items;
        }
        // get serialized tags
        std::string tags_str = serialize_tags(msg_items);
        // create message
        size_t data_bytes = msg_items*d_itemsize*d_vlen;
        size_t tags_bytes = tags_str.size();
        size_t msg_size = sizeof(size_t) + data_bytes + sizeof(size_t) + tags_bytes;
        zmq::message_t msg(msg_size);
        memcpy(  (char *)msg.data(),                                          &data_bytes, sizeof(size_t));
        memcpy(  (char *)msg.data()+sizeof(size_t),                           in,          data_bytes);
        memcpy(  (char *)msg.data()+sizeof(size_t)+data_bytes,                &tags_bytes, sizeof(size_t));
        if (tags_bytes > 0) {
          memcpy((char *)msg.data()+sizeof(size_t)+data_bytes+sizeof(size_t), &tags_str,   tags_bytes);
        }
        // send message
        d_socket->send(msg);

        return msg_items;
      }
      return 0;
    }
  } /* namespace zeromq */
} /* namespace gr */
