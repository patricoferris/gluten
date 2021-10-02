(*----------------------------------------------------------------------------
 *  Copyright (c) 2018 Inhabited Type LLC.
 *  Copyright (c) 2018 Anton Bachin
 *  Copyright (c) 2019-2020 Antonio N. Monteiro.
 *
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *  3. Neither the name of the author nor the names of his contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE CONTRIBUTORS ``AS IS'' AND ANY EXPRESS
 *  OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 *  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 *  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 *  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 *  STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 *  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *---------------------------------------------------------------------------*)

include Gluten_eio_intf

open Eio.Std

(* Borrowed from talex5: https://github.com/talex5/httpaf/blob/d067a3afcbcd65f80531b31699b7a85242c1d6fb/eio/httpaf_eio.ml *)
let cstruct_of_faraday { Faraday.buffer; off; len } = Cstruct.of_bigarray ~off ~len buffer

module IO_loop = struct
  let start
      : type t fd.
        sw:Switch.t 
        -> (module Gluten.RUNTIME with type t = t)
        -> t
        -> read_buffer_size:int
        -> #Eio.Flow.two_way
        -> unit
    =
   fun ~sw (module Runtime) t ~read_buffer_size socket ->
    let read_buffer = Cstruct.create read_buffer_size in
    (* let read_loop_exited, notify_read_loop_exited = Lwt.wait () in *)
    let rec read_loop () =
      let rec read_loop_step () =
        match Runtime.next_read_operation t with
        | `Read -> (
          try 
            let _ : int = Eio.Flow.read_into ~sw socket read_buffer in
            ignore (Runtime.read t read_buffer.buffer read_buffer.off read_buffer.len);
            read_loop_step ()
          with 
            End_of_file -> 
              ignore (Runtime.read_eof t read_buffer.buffer ~off:read_buffer.off ~len:read_buffer.len);
              read_loop_step () )
        | `Yield ->
          Runtime.yield_reader t read_loop
        | `Close ->
          (* Lwt.wakeup_later notify_read_loop_exited (); *)
          Eio.Flow.shutdown socket `Send
          (* Io.shutdown_receive socket *)
          (* Lwt.return_unit *)
      in
      Fibre.fork_ignore ~sw @@ fun () -> 
        try read_loop_step () with exn -> Runtime.report_exn t exn
    in
    let write socket sources = 
      Eio.Flow.copy ~sw (Eio.Flow.cstruct_source (List.map cstruct_of_faraday sources)) socket 
    in
    let write socket io_vectors =
      match write socket io_vectors with
      | () -> `Ok (List.fold_left (fun acc f -> acc + f.Faraday.len) 0 io_vectors)
      | exception Unix.Unix_error (Unix.EPIPE, _, _) -> `Closed
    in
    (* let write_loop_exited, notify_write_loop_exited = Lwt.wait () in *)
    let rec write_loop () =
      let rec write_loop_step () =
        match Runtime.next_write_operation t with
        | `Write io_vectors ->
          let result = write socket io_vectors in
          Runtime.report_write_result t result;
          write_loop_step ()
        | `Yield ->
          Runtime.yield_writer t write_loop
        | `Close _ -> raise Exit
          (* Lwt.wakeup_later notify_write_loop_exited (); *)
          (* Lwt.return_unit *)
      in
      Fibre.fork_ignore ~sw @@ fun () -> 
        try write_loop_step () with exn -> Runtime.report_exn t exn
    in
    read_loop ();
    write_loop ();
    Eio.Flow.shutdown socket `Send
    (* Io.close socket *)
end

module Server = struct
  module Server = Gluten.Server

  type socket = <Eio.Flow.two_way; Eio.Flow.close>

  type addr = Eio.Net.Sockaddr.t

  let create_connection_handler
      ~sw ~read_buffer_size ~protocol connection _client_addr socket
    =
    let connection = Server.create ~protocol connection in
    IO_loop.start
      ~sw
      (module Server)
      connection
      ~read_buffer_size
      socket

  let create_upgradable_connection_handler
      ~sw
      ~read_buffer_size
      ~protocol
      ~create_protocol
      ~request_handler
      client_addr
      socket
    =
    let connection =
      Server.create_upgradable
        ~protocol
        ~create:create_protocol
        (request_handler client_addr)
    in
    IO_loop.start
      ~sw
      (module Server)
      connection
      ~read_buffer_size
      socket
end

module Client = struct
  module Client_connection = Gluten.Client

  type socket = <Eio.Flow.two_way; Eio.Flow.close>

  type t =
    { connection : Client_connection.t
    ; socket : socket;
    }

  let create ~sw ~read_buffer_size ~protocol t socket =
    let connection = Client_connection.create ~protocol t in
    Fibre.fork_ignore ~sw @@ (fun () -> 
        IO_loop.start
          ~sw
          (module Client_connection)
          connection
          ~read_buffer_size
          socket);
    { connection; socket }

  let upgrade t protocol =
    Client_connection.upgrade_protocol t.connection protocol

  let shutdown t =
    Client_connection.shutdown t.connection;
    Eio.Flow.shutdown t.socket `Send

  let is_closed t = Client_connection.is_closed t.connection
end
