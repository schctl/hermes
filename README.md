# _`hermes`_

Experimental Real-Time-Publish-Subscribe (RTPS) framework for embedded communication.

## Motivation

> This is where I pretend to know what I'm talking about and justify why this library exists.

The existence of a simplified communications architecture for resource-constrained real-time systems
seems to be lacking. Many popular solutions out there target extremely wide use cases, with features
like IP-integration and cloud compatibility, these solutions tend to be extremely complex and over
extended for the most simple use cases.

The driving motivation behind _`hermes`_ is that all we need from a high-level communication protocol
is to easily be able to exchange bytes. The exchanged data must be meaningful, can be varied in length
and should be easily distributable over a network. See the [Design Goals](#goals) for more details.

_`hermes`_ as a protocol establishes this low-overhead communications structure, while _`hermes`_ as
a library abstracts over the hardware-level details and provides high-level programming interfaces
to access this structure.

### Why not IP?

IP is probably the best and most robust option you can go with for something like this, but as
a protocol it is also quite heavy. IP packets contain all sorts of metadata, like for routing, TTL, etc.

In consequence, all sorts of protocols that are built on top of IP also end up being too complicated
for what we want in a "wire" protocol.

There is another problem of the IP software stack having limited availability on microcontrollers.
lwIP is one such software stack provided for the STM32 family, but this tends to be restricted to
higher end STM products due to resource requirements and Ethernet availability.

We have the opportunity to make something simpler, applied in our resource constrained use case, i.e
intra-vehicular communication, while also being robust and easy to use.

### Comparison of different protocols / library implementations

| Name                        | IP independent       | Decentralized        | Complexity     | Resources      | Mixed Transport |
| --------------------------- | -------------------- | -------------------- | -------------- | -------------- | ----------------|
| MQTT                        | :x:                  | :x:                  | :white_circle: | :white_circle: | :x:             |
| micro-ROS (_MicroXRCE-DDS_) | :white_check_mark:   | :x:                  | :white_circle: | :white_circle: | :x:             |
| RMW                         | :white_check_mark:   | :white_check_mark:   | :x:            | :x:            | *               |
| UAVCAN                      | :white_check_mark:   | :white_check_mark:   | :white_circle: | :x:            | :x:             |
| eProsima FastRTPS           | :x:                  | :white_check_mark:   | :x:++          | :x:            | :x:             |

_*Implementation specific._

## Design

### Goals

- Decentralized
- Low byte overhead
- Publish / Subscribe semantics
- Efficiently software implementable
- Generic over any hardware protocol - Serial and CAN being the main applications

### Non-goals

- Higher level networking constructs like discovery, domains, etc. The structure of the network must
  be known at compile time. Avoid RTPS constructs like Participants and Domains, etc.
- Type abstractions in the network layer. Protocols like UAVCAN and ROS try to define type associations
  with respect to messages and are built-in to the protocol. If any sort of type associations are even
  necessary, we want them to be handled by the application layer.
