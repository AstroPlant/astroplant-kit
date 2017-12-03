================================
AstroPlant kit documentation
================================

The AstroPlant kit is a framework for plant growth systems, especially the namesake `AstroPlant kit <http://astroplant.io>`_ growth system.
It is designed to be used with an AstroPlant backend, but any backend implementing the API is supported.

About this documentation
========================

This documentation gives the technical details to deploy, change, and
improve the system. If you are looking for information on using the
AstroPlant system, see the 
`AstroPlant build guide <https://astroplant.gitbooks.io/building-an-astroplant-kit>`_.

Project overview
================

The main goal of the kit framework is to be lean, as it is intended to support inexepensive hardware.
For example, the AstroPlant kit uses the inexpensive, single-board Raspberry Pi Zero W.

The AstroPlant kit contains a number of peripheral devices.
A peripheral device is an abstraction of any device, but is intended to have some interaction with the real world.
For example, a peripheral device can be a sensor, such as a barometer, producing temperature, humidity, and air pressure measurements, or it can be an actuator, such as a heater, capable of influencing the growth system's temperature.
A controller (work-in-progress) ties the devices together based on growth recipes; e.g. it controls a heater based on input from a temperature sensor.
    
Installation instructions
=========================

For installation instructions, see the
`project repository <https://github.com/astroplant/astroplant-kit>`_.
    
.. toctree::
   :maxdepth: 1
   :caption: Contents
   
   Modules <modules/modules>
   
Other
=====
   
* :ref:`search`
