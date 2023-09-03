/***************************************************************************
 *   Copyright (C) 2011-2012 by Andreas Frisch                             *
 *   fraxinas@opendreambox.org                                             *
 *                                                                         *
 * This program is licensed under the Creative Commons                     *
 * Attribution-NonCommercial-ShareAlike 3.0 Unported                       *
 * License. To view a copy of this license, visit                          *
 * http://creativecommons.org/licenses/by-nc-sa/3.0/ or send a letter to   *
 * Creative Commons,559 Nathan Abbott Way,Stanford,California 94305,USA.   *
 *                                                                         *
 * Alternatively, this program may be distributed and executed on          *
 * hardware which is licensed by Dream Multimedia GmbH.                    *
 *                                                                         *
 * This program is NOT free software. It is open source, you are allowed   *
 * to modify it (if you keep the license), but it may not be commercially  *
 * distributed other than under the conditions noted above.                *
 *                                                                         *
 ***************************************************************************/

// gcc -Wall -g `pkg-config gstreamer-1.0 --cflags --libs` bdremux.c -o bdremux

#include <gst/gst.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <glib.h>
#include <glib/gprintf.h>
#include <getopt.h>

#include <byteswap.h>
// #include <netinet/in.h>

#ifndef BYTE_ORDER
#error no byte order defined!
#endif

#define CLOCK_BASE 9LL
#define CLOCK_FREQ (CLOCK_BASE * 10000)

#define MPEGTIME_TO_GSTTIME(time) (gst_util_uint64_scale ((time), \
            GST_MSECOND/10, CLOCK_BASE))
#define GSTTIME_TO_MPEGTIME(time) (gst_util_uint64_scale ((time), \
            CLOCK_BASE, GST_MSECOND/10))

#define MAX_PIDS 8
#define DEFAULT_QUEUE_SIZE 48*1024*1024

GST_DEBUG_CATEGORY (bdremux_debug);
#define GST_CAT_DEFAULT bdremux_debug

typedef struct _App App;

typedef struct _Segment
{
  int index;
  guint64 in_pts;
  guint64 out_pts;
} segment_t;

struct _App
{
  gchar *in_filename;
  gchar *out_filename;
  gchar *cuts_filename;
  gchar *epmap_filename;
  gboolean enable_indexing;
  gboolean enable_cutlist;
  GstElement *pipeline;
  GstElement *filesrc;
  GstElement *tsdemux;
  GstElement *queue;
  GstElement *videoparser;
  GstElement *audioparsers[MAX_PIDS];
  GstElement *m2tsmux;
  GstElement *filesink;
  gulong buffer_handler_id;
  gint a_source_pids[MAX_PIDS], a_sink_pids[MAX_PIDS];
  gulong probe_ids[MAX_PIDS];
  guint source_pids_count, sink_pids_count;
  guint requested_pid_count;
  gboolean auto_pids;

  GMainLoop *loop;
  gboolean is_seekable;
  int current_segment;
  int segment_count;
  segment_t *seek_segments;

  guint spn_count;

  guint queue_cb_handler_id;
  guint queue_size;

  FILE *f_epmap;
};

App s_app;

static void
bdremux_errout (gchar * string)
{
  g_fprintf (stdout, "ERROR: %s\n", string);
  fflush (stdout);
  GST_ERROR ("%s", string);
  exit (1);
}

static gboolean
load_cutlist (App * app)
{
  FILE *f;
  int segment_i = 0;

  f = fopen (app->cuts_filename, "rb");

  if (f) {
    GST_INFO ("cutfile found! loading cuts...");
    while (1) {
      unsigned long long where;
      unsigned int what;

      if (!fread (&where, sizeof (where), 1, f))
        break;
      if (!fread (&what, sizeof (what), 1, f))
        break;

#if BYTE_ORDER == LITTLE_ENDIAN
      where = bswap_64 (where);
#endif
      what = ntohl (what);
      GST_DEBUG ("where= %lld, what=%i", where, what);
      if (what > 3)
        break;

      if (what == 0) {
        app->segment_count++;
        app->seek_segments =
            (segment_t *) realloc (app->seek_segments,
            app->segment_count * sizeof (segment_t));
        app->seek_segments[segment_i].index = segment_i;
        app->seek_segments[segment_i].in_pts = where;
        app->seek_segments[segment_i].out_pts = -1;
      }
      if (what == 1 && segment_i < app->segment_count) {
        app->seek_segments[segment_i].out_pts = where;
        segment_i++;
      }
    }
    fclose (f);
  } else
    GST_WARNING ("cutfile not found!");
// 
  return TRUE;
}

static gboolean
do_seek (App * app)
{
  gint64 in_pos, out_pos;
  gfloat rate = 1.0;
  GstSeekFlags flags = 0;
  int ret;

  if (app->current_segment >= app->segment_count) {
    GST_WARNING ("seek segment not found!");
    return FALSE;
  }

  GST_INFO ("do_seek...");
  flags |= GST_SEEK_FLAG_FLUSH;
//      flags |= GST_SEEK_FLAG_ACCURATE;
  flags |= GST_SEEK_FLAG_KEY_UNIT;
//   if (app->segment_count > 1)
    flags |= GST_SEEK_FLAG_SEGMENT;

  gst_element_query_position ((app->pipeline), GST_FORMAT_TIME, &in_pos);
  GST_DEBUG ("do_seek::initial gst_element_query_position = %" G_GUINT64_FORMAT " ms",
      in_pos / 1000000);

  in_pos =
      MPEGTIME_TO_GSTTIME (app->seek_segments[app->current_segment].in_pts);
  GST_DEBUG ("do_seek::in_time for segment %i = %" G_GUINT64_FORMAT " ms", app->current_segment,
      in_pos / 1000000);

  guint64 out_pts = app->seek_segments[app->current_segment].out_pts;
  out_pos = (out_pts == -1) ? out_pts : MPEGTIME_TO_GSTTIME (app->seek_segments[app->current_segment].out_pts);
  GST_DEBUG ("do_seek::out_time for segment %i = %" G_GUINT64_FORMAT " ms (pts=%" G_GUINT64_FORMAT ")", app->current_segment,
      out_pos / 1000000, out_pts);

  ret = gst_element_seek ((app->pipeline), rate, GST_FORMAT_TIME, flags,
      GST_SEEK_TYPE_SET, in_pos, GST_SEEK_TYPE_SET, out_pos);

  gst_element_query_position ((app->pipeline), GST_FORMAT_TIME, &in_pos);
  GST_DEBUG
      ("do_seek::seek command returned %i. new gst_element_query_position = %" G_GUINT64_FORMAT " ms",
      ret, in_pos / 1000000);

  if (ret)
    app->current_segment++;

  return ret;
}

static GstPadProbeReturn
buffer_probe_cb (GstPad * pad, GstPadProbeInfo * info, App * app)
{
  static guint64 total_size = 0;
  static gsize calc_spu = 0;
  GST_DEBUG ("buffer_probe_cb pad=%s:%s info type=%i data is buffer?%i", GST_DEBUG_PAD_NAME (pad), info->type, GST_IS_BUFFER(info->data));
  GstBuffer *buffer = GST_BUFFER (info->data);
  GstMapInfo map;
  gst_buffer_map (buffer, &map, GST_MAP_READ);
  guint8 *data = map.data;
  gsize data_len = map.size;
  total_size += data_len;
  calc_spu += (data_len / 192);
//   GST_DEBUG
//       ("buffer_probe_cb pad=%s:%s info type=%i, data@%p size=%i (total=%llu) spn_count=%i spn_in_this_buffer=%i (total=%i) buffer data 0x %02X %02X %02X %02X %02X %02X %02X %02X",
//       GST_DEBUG_PAD_NAME (pad), info->type, info->data, data_len, total_size,
//       app->spn_count, (data_len / 192), calc_spu, data[0], data[1], data[2],
//       data[3], data[4], data[5], data[6], data[7]);
  gsize offset = 0;
  guint64 pts = 0;
  unsigned char *payload;
  guint8 *pkt, *end;

  while (data_len >= 192) {
    app->spn_count++;
    data_len -= 192;

    if (offset)
      offset += 192;
    else
      offset += 4;

    int pusi = ! !(data[offset + 1] & 0x40);
    if (!pusi) {
      continue;
    }

    int pid = ((data[offset + 1] << 8) | data[offset + 2]) & 0x1FFF;

    if (pid != app->a_sink_pids[0]) {
//          GST_DEBUG ("drop... no video!");
      continue;
    }

    /* check for adaption field */
    if (data[offset + 3] & 0x20) {
      payload = data + offset + data[offset + 4] + 4 + 1;
    } else
      payload = data + offset + 4;


    /* somehow not a startcode. (this is invalid, since pusi was set.) ignore it. */
    if (payload[0] || payload[1] || (payload[2] != 1)) {
      continue;
    }

    /* drop non-audio, non-video packets because other streams
       can be non-compliant. */
    if (((payload[3] & 0xE0) != 0xC0) &&        // audio
        ((payload[3] & 0xF0) != 0xE0))  // video
    {
//          GST_DEBUG ("drop non-audio or -video");
      continue;
    }

    if (payload[7] & 0x80) {    /* PTS */
      pts = ((unsigned long long) (payload[9] & 0xE)) << 29;
      pts |= ((unsigned long long) (payload[10] & 0xFF)) << 22;
      pts |= ((unsigned long long) (payload[11] & 0xFE)) << 14;
      pts |= ((unsigned long long) (payload[12] & 0xFF)) << 7;
      pts |= ((unsigned long long) (payload[13] & 0xFE)) >> 1;
    } else
      pts = 0;

    pkt = payload + payload[8] + 9;
    end = payload + 188;
    while (pkt < (end - 4)) {
      if (!pkt[0] && !pkt[1] && pkt[2] == 1) {
        if (pkt[3] == 0x09 &&   /* MPEG4 AVC NAL unit access delimiter */
            (pkt[4] >> 5) == 0 && pts) {        /* and I-frame */
          g_fprintf (app->f_epmap, "entrypoint: %i %" G_GINT64_FORMAT "\n",
              app->spn_count, pts);
          fflush (app->f_epmap);
        }
      }
      ++pkt;
    }
  }

  return GST_PAD_PROBE_OK;

}


static GstPadProbeReturn
pad_block_cb (GstPad * pad, GstPadProbeInfo * info, App * app)
{
  GST_DEBUG ("pad_block_cb %s:%s info type=%i", GST_DEBUG_PAD_NAME (pad),
      info->type);
//   return GST_PAD_PROBE_PASS;
  return GST_PAD_PROBE_OK;
}

static gboolean
bus_message (GstBus * bus, GstMessage * message, App * app)
{
  gchar *sourceName;
  GstObject *source;
  gchar *string;

  if (!message)
    return FALSE;
  source = GST_MESSAGE_SRC (message);
  if (!GST_IS_OBJECT (source))
    return FALSE;
  sourceName = gst_object_get_name (source);

  if (gst_message_get_structure (message))
    string = gst_structure_to_string (gst_message_get_structure (message));
  else
    string = g_strdup (GST_MESSAGE_TYPE_NAME (message));
  GST_DEBUG ("gst_message from %s: %s", sourceName, string);
  g_free (string);

  switch (GST_MESSAGE_TYPE (message)) {
    case GST_MESSAGE_ERROR:
    {
      GError *gerror;
      gchar *debug;

      gst_message_parse_error (message, &gerror, &debug);
      gst_object_default_error (GST_MESSAGE_SRC (message), gerror, debug);
      g_error_free (gerror);
      g_free (debug);

      g_main_loop_quit (app->loop);
      break;
    }
    case GST_MESSAGE_WARNING:
    {
      GError *gerror;
      gchar *debug;

      gst_message_parse_warning (message, &gerror, &debug);
      gst_object_default_error (GST_MESSAGE_SRC (message), gerror, debug);
      g_error_free (gerror);
      g_free (debug);

//       g_main_loop_quit (app->loop);
      break;
    }
    case GST_MESSAGE_EOS:
      g_message ("received EOS");
      g_main_loop_quit (app->loop);
      break;
    case GST_MESSAGE_ASYNC_DONE:
      break;
    case GST_MESSAGE_ELEMENT:
    {
      const GstStructure *msgstruct = gst_message_get_structure (message);
      if (msgstruct) {
        const gchar *eventname = gst_structure_get_name (msgstruct);
        if (!strcmp (eventname, "seekable"))
          app->is_seekable = TRUE;
      }
      break;
    }
    case GST_MESSAGE_STATE_CHANGED:
    {
      GstState old_state, new_state;
      GstStateChange transition;

      gst_message_parse_state_changed (message, &old_state, &new_state, NULL);
      transition = (GstStateChange) GST_STATE_TRANSITION (old_state, new_state);

      switch (transition) {
        case GST_STATE_CHANGE_NULL_TO_READY:
          break;
        case GST_STATE_CHANGE_READY_TO_PAUSED:
          break;
        case GST_STATE_CHANGE_PAUSED_TO_PLAYING:
        {
	  if (GST_MESSAGE_SRC (message) == GST_OBJECT (app->pipeline))
	  {
	     if (app->current_segment == 0 && app->segment_count )
	           do_seek (app);
	  }
	  else if (GST_MESSAGE_SRC (message) == GST_OBJECT (app->m2tsmux))
	  {
	    if (app->enable_indexing) {
	      GstPad *mux_srcpad = NULL;
	      mux_srcpad = gst_element_get_static_pad (app->m2tsmux, "src");
	      gst_pad_add_probe (mux_srcpad, GST_PAD_PROBE_TYPE_BUFFER,
		  (GstPadProbeCallback) buffer_probe_cb, app, NULL);
	    }
	  }
        }
          break;
        case GST_STATE_CHANGE_PLAYING_TO_PAUSED:
          break;
        case GST_STATE_CHANGE_PAUSED_TO_READY:
          break;
        case GST_STATE_CHANGE_READY_TO_NULL:
          break;
      }
      break;
    }
    case GST_MESSAGE_SEGMENT_DONE:
    {
      GST_DEBUG ("GST_MESSAGE_SEGMENT_DONE!!!");
      do_seek (app);
    }
    default:
      break;
  }
  GST_DEBUG_BIN_TO_DOT_FILE (GST_BIN (app->pipeline), GST_DEBUG_GRAPH_SHOW_ALL,
      "bdremux_pipelinegraph_message");
  return TRUE;
}

static void
mux_pad_has_caps_cb (GstPad * pad, GParamSpec * unused, App * app)
{
  GST_DEBUG ("mux_pad_has_caps_cb %s:%s", GST_DEBUG_PAD_NAME (pad));
  GstCaps *caps;

  g_object_get (G_OBJECT (pad), "caps", &caps, NULL);

  if (caps) {
    g_fprintf (stdout, "%s:%s has CAPS: %s\n", GST_DEBUG_PAD_NAME (pad),
        gst_caps_to_string (caps));
    fflush (stdout);
    gst_caps_unref (caps);
  }
}

static void
_unblock_pads (App * app)
{
  int i;
  gchar srcpadname[9];
  GstPad *queue_srcpad;
  for (i = 0; i < app->sink_pids_count; i++) {
    if (app->probe_ids[i] > 0) {
      g_sprintf (srcpadname, "src_%d", app->a_sink_pids[i]);
      queue_srcpad = gst_element_get_static_pad (app->queue, srcpadname);
      gst_pad_remove_probe (queue_srcpad, app->probe_ids[i]);
      GST_DEBUG ("UNBLOCKING %s (id %lu)", srcpadname, app->probe_ids[i]);
      app->probe_ids[i] = 0;
      gst_object_unref (queue_srcpad);
    }
  }
}

static void
queue_filled_cb (GstElement * element, App * app)
{
  GST_DEBUG ("queue_filled_cb");

  if (app->auto_pids) {
    GST_INFO
        ("First time queue overrun and auto_pids -> UNBLOCKING all pads and start muxing! (have %i PIDs @ mux)",
        app->requested_pid_count);
    _unblock_pads (app);
  }
  else if (app->requested_pid_count == app->sink_pids_count) {
    GST_INFO
        ("First time queue overrun and requested PIDs count (%i) reached number of sink PIDs -> UNBLOCKING all pads and start muxing!",
        app->requested_pid_count);  
    _unblock_pads (app);
  }
  if (app->requested_pid_count >= app->sink_pids_count)
  {
    GST_INFO
        ("Disconnect queue_filled_cb! requested_pid_count=%i, sink_pids_count=%i", app->requested_pid_count, app->sink_pids_count);
    g_signal_handler_disconnect (G_OBJECT (element), app->queue_cb_handler_id);
  }
}

static void
demux_unblock_pads (GstElement * element, App * app)
{
  GST_INFO
      ("Demux posted no-more-pads signal -> UNBLOCKING all pads and start muxing! (have %i PIDS @ mux)",
      app->requested_pid_count);
  _unblock_pads (app);
}

static void
demux_pad_added_cb (GstElement * element, GstPad * demuxpad, App * app)
{
  GstPad *parser_sinkpad = NULL, *parser_srcpad = NULL, *queue_sinkpad =
      NULL, *queue_srcpad = NULL, *mux_sinkpad = NULL;
  GstStructure *s;
  GstCaps *filter_caps = NULL, *caps = gst_pad_get_current_caps (demuxpad);
  GstElement *videocapsfilter = NULL;

  gchar *demuxpadname, sinkpadname[10], srcpadname[9];
  guint sourcepid;
  int i;

  s = gst_caps_get_structure (caps, 0);
  demuxpadname = gst_pad_get_name (demuxpad);
  GST_DEBUG ("demux_pad_added_cb %s:%s", GST_DEBUG_PAD_NAME (demuxpad));

  if (g_ascii_strncasecmp (demuxpadname, "video", 5) == 0) {
    sscanf (demuxpadname + 6, "%x", &sourcepid);
    if (app->auto_pids) {
      app->a_source_pids[0] = sourcepid;
      if (app->a_sink_pids[0] == -1)
        app->sink_pids_count++;
      app->a_sink_pids[0] = sourcepid;
      app->source_pids_count++;
    }
    if (sourcepid == app->a_source_pids[0] && app->videoparser == NULL) {
      if (gst_structure_has_name (s, "video/mpeg")) {
        app->videoparser =
            gst_element_factory_make ("mpegvideoparse", "videoparse");
        if (!app->videoparser) {
          bdremux_errout
              ("mpegvideoparse not found! please install gst-plugin-mpegvideoparse!");
        }
      } else if (gst_structure_has_name (s, "video/x-h264")) {
        app->videoparser = gst_element_factory_make ("h264parse", "videoparse");
        if (!app->videoparser) {
          bdremux_errout
              ("h264parse not found! please install gst-plugin-videoparsersbad!");
        }
        videocapsfilter =
            gst_element_factory_make ("capsfilter", "videocapsfilter");
        filter_caps =
            gst_caps_from_string ("video/x-h264, alignment=(string)au");
        g_object_set (G_OBJECT (videocapsfilter), "caps", filter_caps, NULL);
      }
      gst_bin_add (GST_BIN (app->pipeline), app->videoparser);

      parser_sinkpad = gst_element_get_static_pad (app->videoparser, "sink");
      parser_srcpad = gst_element_get_static_pad (app->videoparser, "src");

      if (videocapsfilter) {
        GstPad *filter_sinkpad = NULL;
        gst_bin_add (GST_BIN (app->pipeline), videocapsfilter);
        filter_sinkpad = gst_element_get_static_pad (videocapsfilter, "sink");

        if (gst_pad_link (parser_srcpad, filter_sinkpad) != 0) {
          bdremux_errout (g_strdup_printf ("Couldn't link %s:%s to %s:%s",
                  GST_DEBUG_PAD_NAME (parser_srcpad),
                  GST_DEBUG_PAD_NAME (filter_sinkpad)));
        }
        GST_INFO ("************initialized and linked capsfilter OK");
        parser_srcpad = gst_element_get_static_pad (videocapsfilter, "src");
        GST_DEBUG_BIN_TO_DOT_FILE (GST_BIN (app->pipeline),
            GST_DEBUG_GRAPH_SHOW_ALL, "bdremux_pipelinegraph_filter_added");
        gst_object_unref (filter_sinkpad);
      }

      g_sprintf (sinkpadname, "sink_%d", app->a_sink_pids[0]);
      g_sprintf (srcpadname, "src_%d", app->a_sink_pids[0]);
      queue_sinkpad = gst_element_get_request_pad (app->queue, sinkpadname);
      queue_srcpad = gst_element_get_static_pad (app->queue, srcpadname);
      g_sprintf (sinkpadname, "sink_%d", app->a_sink_pids[0]);
      mux_sinkpad = gst_element_get_request_pad (app->m2tsmux, sinkpadname);
      app->requested_pid_count++;
      if (app->requested_pid_count <= app->source_pids_count) {
        GST_DEBUG ("gst_pad_add_probe on %s:%s requested_pid_count=%i",
            GST_DEBUG_PAD_NAME (queue_srcpad), app->requested_pid_count);
        app->probe_ids[0] =
            gst_pad_add_probe (queue_srcpad,
            GST_PAD_PROBE_TYPE_BLOCK_DOWNSTREAM,
            (GstPadProbeCallback) pad_block_cb, app, NULL);
        GST_DEBUG ("BLOCKING %s returned %lu", srcpadname, app->probe_ids[0]);
      }
      if (gst_pad_link (demuxpad, parser_sinkpad) == 0) {
        if (gst_pad_link (parser_srcpad, queue_sinkpad) == 0) {
          if (gst_pad_link (queue_srcpad, mux_sinkpad) == 0) {
            g_fprintf
                (stdout, "linked: Source PID %d to %s\n",
                app->a_source_pids[0], sinkpadname);
            gst_element_set_state (videocapsfilter, GST_STATE_PLAYING);
            gst_element_set_state (app->videoparser, GST_STATE_PLAYING);
            g_signal_connect (G_OBJECT (mux_sinkpad), "notify::caps",
                G_CALLBACK (mux_pad_has_caps_cb), app);
            fflush (stdout);
          } else {
            bdremux_errout (g_strdup_printf ("Couldn't link %s:%s to %s:%s",
                    GST_DEBUG_PAD_NAME (queue_srcpad),
                    GST_DEBUG_PAD_NAME (mux_sinkpad)));
          }
        } else {
          bdremux_errout (g_strdup_printf ("Couldn't link %s:%s to %s:%s @%p",
                  GST_DEBUG_PAD_NAME (parser_srcpad),
                  GST_DEBUG_PAD_NAME (queue_sinkpad), queue_sinkpad));
        }
      } else {
        bdremux_errout (g_strdup_printf ("Couldn't link %s:%s to %s:%s",
                GST_DEBUG_PAD_NAME (demuxpad),
                GST_DEBUG_PAD_NAME (parser_sinkpad)));
      }
    }
  } else if (g_ascii_strncasecmp (demuxpadname, "audio", 5) == 0) {
    sscanf (demuxpadname + 6, "%x", &sourcepid);
    if (app->auto_pids) {
      if (app->source_pids_count == 0)
        i = 1;
      else
        i = app->source_pids_count;
      app->a_source_pids[i] = sourcepid;
      if (app->a_sink_pids[i] == -1) {
        app->a_sink_pids[i] = sourcepid;
        app->sink_pids_count++;
      }
      app->source_pids_count++;
    }
    for (i = 1; i < app->source_pids_count; i++) {
      if (sourcepid == app->a_source_pids[i]) {
        if (gst_structure_has_name (s, "audio/mpeg")) {
          app->audioparsers[i] =
              gst_element_factory_make ("mpegaudioparse", NULL);
          if (!app->audioparsers[i]) {
            bdremux_errout
                ("mpegaudioparse not found! please install gst-plugin-mpegaudioparse!");
          }
        } else if (gst_structure_has_name (s, "audio/x-ac3")) {
          app->audioparsers[i] = gst_element_factory_make ("ac3parse", NULL);
          if (!app->audioparsers[i]) {
            bdremux_errout
                ("mpegaudioparse not found! please install gst-plugin-audioparses!");
          }
        } else if (gst_structure_has_name (s, "audio/x-dts")) {
          app->audioparsers[i] = gst_element_factory_make ("dcaparse", NULL);
          if (!app->audioparsers[i]) {
            bdremux_errout
                ("dcaparse not found! please install gst-plugin-audioparses!");
          }
        } else {
          bdremux_errout (g_strdup_printf
              ("could not find parser for audio stream with pid 0x%04x!",
                  sourcepid));
        }

        gst_bin_add (GST_BIN (app->pipeline), app->audioparsers[i]);
        parser_sinkpad =
            gst_element_get_static_pad (app->audioparsers[i], "sink");
        parser_srcpad =
            gst_element_get_static_pad (app->audioparsers[i], "src");
        g_sprintf (sinkpadname, "sink_%d", app->a_sink_pids[i]);
        g_sprintf (srcpadname, "src_%d", app->a_sink_pids[i]);
        queue_sinkpad = gst_element_get_request_pad (app->queue, sinkpadname);
        queue_srcpad = gst_element_get_static_pad (app->queue, srcpadname);
        g_sprintf (sinkpadname, "sink_%d", app->a_sink_pids[i]);
        mux_sinkpad = gst_element_get_request_pad (app->m2tsmux, sinkpadname);
        app->requested_pid_count++;
        if (app->requested_pid_count <= app->source_pids_count) {
          GST_DEBUG ("gst_pad_add_probe %i on %s:%s", i,
              GST_DEBUG_PAD_NAME (queue_srcpad));
          app->probe_ids[i] =
              gst_pad_add_probe (queue_srcpad,
              GST_PAD_PROBE_TYPE_BLOCK_DOWNSTREAM,
              (GstPadProbeCallback) pad_block_cb, app, NULL);
          GST_DEBUG ("BLOCKING %s returned %lu", srcpadname, app->probe_ids[i]);
        }
        if (gst_pad_link (demuxpad, parser_sinkpad) == 0
            && gst_pad_link (parser_srcpad, queue_sinkpad) == 0
            && gst_pad_link (queue_srcpad, mux_sinkpad) == 0) {
          g_print
              ("linked: Source PID %d to %s\n",
              app->a_source_pids[i], sinkpadname);
          gst_element_set_state (app->audioparsers[i], GST_STATE_PLAYING);
          g_signal_connect (G_OBJECT (mux_sinkpad), "notify::caps",
              G_CALLBACK (mux_pad_has_caps_cb), app);
        } else
          bdremux_errout (g_strdup_printf
              ("Couldn't link audio PID 0x%04x to sink PID 0x%04x",
                  app->a_source_pids[i], app->a_sink_pids[i]));
        break;
      }
    }
  } else
    GST_INFO ("Ignoring pad %s!", demuxpadname);

  if (parser_sinkpad)
    gst_object_unref (parser_sinkpad);
  if (parser_srcpad)
    gst_object_unref (parser_srcpad);
  if (queue_sinkpad)
    gst_object_unref (queue_sinkpad);
  if (queue_srcpad)
    gst_object_unref (queue_srcpad);
  if (mux_sinkpad)
    gst_object_unref (mux_sinkpad);
  if (caps)
    gst_caps_unref (caps);
  if (filter_caps)
    gst_caps_unref (filter_caps);

  g_free (demuxpadname);
  GST_DEBUG_BIN_TO_DOT_FILE (GST_BIN (app->pipeline), GST_DEBUG_GRAPH_SHOW_ALL,
      "bdremux_pipelinegraph_pad_added");
}

static gint
get_pid (gchar * s_pid)
{
  gint pid = -1;
  if (g_ascii_strncasecmp (s_pid, "0x", 2) == 0) {
    sscanf (s_pid + 2, "%x", &pid);
    return pid;
  } else
    return atoi (s_pid);
}

static void
parse_pid_list (gint * array, guint * count, char *string)
{
  gchar **split;
  gint i = 0;
  if (!string)
    return;
  GST_DEBUG ("parse_pid_list %s, count=%i", string, *count);
  split = g_strsplit_set (string, ".,", MAX_PIDS);
  for (i = 0; i < MAX_PIDS; i++) {
    if (!split[i] || strlen (split[i]) == 0)
      break;
    array[*count] = get_pid (split[i]);
    (*count)++;
  }
  g_strfreev (split);
  GST_DEBUG ("parse_pid_list %s, count=%i", string, *count);
}

static gboolean
parse_options (int argc, char *argv[], App * app)
{
  int opt;

  const gchar *optionsString = "vecq:s:r:?";
  struct option optionsTable[] = {
    {"entrypoints", optional_argument, NULL, 'e'},
    {"cutlist", optional_argument, NULL, 'c'},
    {"queue-size", required_argument, NULL, 'q'},
    {"source-pids", required_argument, NULL, 's'},
    {"result-pids", required_argument, NULL, 'r'},
    {"help", no_argument, NULL, '?'},
    {"version", no_argument, NULL, 'v'},
    {NULL, 0, NULL, 0}
  };

  if (argc == 1)
    goto usage;

  if (argc > 2)
    app->in_filename = g_strdup (argv[1]);
  app->out_filename = g_strdup (argv[2]);

  while ((opt =
          getopt_long (argc, argv, optionsString, optionsTable, NULL)) >= 0) {
    switch (opt) {
      case 'e':
        app->enable_indexing = TRUE;
        if (optarg != NULL) {
          app->epmap_filename = g_strdup (optarg);
          GST_DEBUG ("arbitrary epmap_filename=%s", app->epmap_filename);
        } else {
          GST_DEBUG ("display ep map on stdout");
        }
        break;
      case 'c':
        app->enable_cutlist = TRUE;
        if (optarg != NULL) {
          app->cuts_filename = g_strdup (optarg);
          GST_DEBUG ("arbitrary cuts_filename=%s", app->cuts_filename);
        } else {
          app->cuts_filename = g_strconcat (app->in_filename, ".cuts", NULL);
          GST_DEBUG ("enigma2-style cuts_filename=%s", app->cuts_filename);
        }

        break;
      case 'q':
        app->queue_size = atoi (optarg);
        GST_DEBUG ("arbitrary queue size=%i", app->queue_size);
        break;
      case 's':
        parse_pid_list (app->a_source_pids, &app->source_pids_count, optarg);
        app->auto_pids = FALSE;
        break;
      case 'r':
        parse_pid_list (app->a_sink_pids, &app->sink_pids_count, optarg);
        break;
      case 'v':
      {
        const gchar *nano_str;
        guint major, minor, micro, nano;
        gst_version (&major, &minor, &micro, &nano);

        if (nano == 1)
          nano_str = "(GIT)";
        else if (nano == 2)
          nano_str = "(Prerelease)";
        else
          nano_str = "";

        g_print ("bdremux 0.1 is linked against GStreamer %d.%d.%d %s\n",
            major, minor, micro, nano_str);
        exit (0);
      }
      case '?':
        goto usage;
        break;
      default:
        break;
    }
  }
  return TRUE;

usage:
  g_print
      ("bdremux - a blu-ray movie stream remuxer <fraxinas@opendreambox.org>\n"
      "\n"
      "Usage: %s source_stream.ts output_stream.m2ts [OPTION...]\n"
      "\n"
      "Optional arguments:\n"
      "  -e, --entrypoints               Generate and display the SPN/PTS map\n"
      "  -c, --cutlist                   use enigma2's $source_stream.ts.cuts file\n"
      "  -q, --queue-size=INT            max size of queue in bytes (default=%i)\n"
      "  -s, --source-pids=STRING        list of PIDs to be considered\n"
      "  -r, --result-pids=STRING        list of PIDs in resulting stream\n"
      "     PIDs can be supplied in decimal or hexadecimal form (0x prefixed)\n"
      "     the lists are supposed to be comma-seperated with the Video PID\n"
      "     as the first element followed by 1-7 Audio PIDs.\n"
      "     If omitted, the first video and all audio elementary streams are\n"
      "     carried over, keeping their PIDs (this may require a larger queue size).\n"
      "\n"
      "Help options:\n"
      "  -?, --help                      Show this help message\n"
      "  -v, --version                   Display GSTREAMER version\n"
      "\n"
      "Example: %s in.ts out.m2ts -e -s0x40,0x4A,0x4C -r0x1011,0x1100,0x1101\n"
      "  Will extract the video elementary stream with PID 0x40 and the audio\n"
      "  streams with PIDs 0x41 and 0x4C from the file in.ts and write new\n"
      "  remultiplexed streams with PID numbers 0x1011 for video and 0x1100\n"
      "  and 0x1101 for audio into the file out.m2ts while showing a map\n"
      "  of entrypoints on stdout.\n", argv[0], DEFAULT_QUEUE_SIZE, argv[0]);
  exit (0);
  return TRUE;
}

int
main (int argc, char *argv[])
{
  App *app = &s_app;
  GstBus *bus;
  int i;

  app->is_seekable = FALSE;
  app->enable_cutlist = FALSE;
  app->segment_count = 0;
  app->current_segment = 0;

  app->epmap_filename = NULL;
  app->f_epmap = NULL;

  app->source_pids_count = 0;
  app->sink_pids_count = 0;
  app->requested_pid_count = 0;
  app->auto_pids = TRUE;
  for (i = 0; i < MAX_PIDS; i++) {
    app->a_sink_pids[i] = -1;
    app->probe_ids[i] = -1;
  }
  app->queue_size = DEFAULT_QUEUE_SIZE;
  app->spn_count = 0;

  gst_init (NULL, NULL);
  GST_DEBUG_CATEGORY_INIT (bdremux_debug, "BDREMUX",
      GST_DEBUG_BOLD | GST_DEBUG_FG_YELLOW | GST_DEBUG_BG_BLUE,
      "blu-ray movie stream remuxer");
  parse_options (argc, argv, app);


  if (app->epmap_filename) {
    app->f_epmap = fopen (app->epmap_filename, "w");
  } else
    app->f_epmap = stdout;

  if (!app->f_epmap) {
    bdremux_errout (g_strdup_printf
        ("could not open %s for writing entry point map! (%i)",
            app->epmap_filename, errno));
  }

  if (app->enable_cutlist)
    load_cutlist (app);

  for (i = 0; i < app->segment_count; i++) {
    GST_INFO ("segment count %i index %i in_pts %" G_GUINT64_FORMAT " out_pts %" G_GUINT64_FORMAT, i,
        app->seek_segments[i].index, app->seek_segments[i].in_pts,
        app->seek_segments[i].out_pts);
  }

  for (i = 0; i < app->source_pids_count; i++) {
    if (app->sink_pids_count <= i)
      app->a_sink_pids[app->sink_pids_count++] = app->a_source_pids[i];
    GST_DEBUG
        ("source pid [%i] = 0x%04x, sink pid [%i] = 0x%04x app->sink_pids_count=%i",
        i, app->a_source_pids[i], i, app->a_sink_pids[i], app->sink_pids_count);
  }

  app->loop = g_main_loop_new (NULL, TRUE);

  app->pipeline = gst_pipeline_new ("blu-ray movie stream remuxer");
  g_assert (app->pipeline);

  app->filesrc = gst_element_factory_make ("filesrc", "filesrc");
  app->tsdemux = gst_element_factory_make ("tsdemux", "tsdemux");
  if (!app->tsdemux) {
    bdremux_errout
        ("mpegtsdemux not found! please install gst-plugin-mpegtsdemux!");
  }

  app->m2tsmux = gst_element_factory_make ("mpegtsmux", "m2tsmux");
  if (!app->m2tsmux) {
    bdremux_errout
        ("mpegtsmux not found! please install gst-plugin-mpegtsmux!");
  }

  app->filesink = gst_element_factory_make ("filesink", "filesink");

  app->queue = gst_element_factory_make ("multiqueue", "multiqueue");

  app->videoparser = NULL;

  gst_bin_add_many (GST_BIN (app->pipeline), app->filesrc, app->tsdemux,
      app->queue, app->m2tsmux, app->filesink, NULL);

  g_object_set (G_OBJECT (app->filesrc), "location", app->in_filename, NULL);

  g_object_set (G_OBJECT (app->queue), "max-size-bytes", app->queue_size, NULL);
  g_object_set (G_OBJECT (app->queue), "max-size-buffers", 0, NULL);
  g_object_set (G_OBJECT (app->queue), "max-size-time", 0, NULL);

  g_object_set (G_OBJECT (app->m2tsmux), "m2ts-mode", TRUE, NULL);
  g_object_set (G_OBJECT (app->m2tsmux), "alignment", 32, NULL);

  g_object_set (G_OBJECT (app->filesink), "location", app->out_filename, NULL);

  gst_element_link (app->filesrc, app->tsdemux);

  gst_element_link (app->m2tsmux, app->filesink);

  g_signal_connect (app->tsdemux, "pad-added", G_CALLBACK (demux_pad_added_cb),
      app);

  g_signal_connect (app->tsdemux, "no-more-pads",
      G_CALLBACK (demux_unblock_pads), app);

  app->queue_cb_handler_id =
      g_signal_connect (app->queue, "overrun", G_CALLBACK (queue_filled_cb),
      app);

  bus = gst_pipeline_get_bus (GST_PIPELINE (app->pipeline));

  gst_bus_add_watch (bus, (GstBusFunc) bus_message, app);

  gst_element_set_state (app->pipeline, GST_STATE_PLAYING);

  g_main_loop_run (app->loop);

  g_message ("stopping");

  gst_element_set_state (app->pipeline, GST_STATE_NULL);

  gst_object_unref (bus);
  g_main_loop_unref (app->loop);

  if (app->epmap_filename) {
    fclose (app->f_epmap);
  }

  return 0;
}
