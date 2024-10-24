#*******************************************************************************
# ALMA - Atacama Large Millimeter Array
# Copyright (c) NAOJ - National Astronomical Observatory of Japan, 2011
# (in the framework of the ALMA collaboration).
# All rights reserved.
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
#*******************************************************************************
#
# $Revision: 1.3.2.2 $
# $Date: 2012/04/02 15:32:42 $
# $Author: tnakazat $
#
INVALID_STAT = -1

SDFlagRule = {\
    'TsysFlag':\
        {'isActive': True, \
         'Reversible': False, \
         'Threshold': 3.0}, \
    'RmsPreFitFlag':\
        {'isActive': True, \
         'Reversible': True, \
         'Threshold': 9.0}, \
    'RmsPostFitFlag':\
        {'isActive': True, \
         'Reversible': True, \
         'Threshold': 8.0}, \
    'RunMeanPreFitFlag':\
        {'isActive': True, \
         'Reversible': True, \
         'Threshold': 11.0, \
         'Nmean': 5}, \
    'RunMeanPostFitFlag':\
        {'isActive': True, \
         'Reversible': True, \
         'Threshold': 10.0, \
         'Nmean': 5}, \
    'RmsExpectedPreFitFlag':\
        {'isActive': True, \
         'Reversible': True, \
         'Threshold': 6.0}, \
    'RmsExpectedPostFitFlag':\
        {'isActive': True, \
         'Reversible': True, \
         'Threshold':  2.6666}, \
    'Flagging':\
        {'ApplicableDuration': 'raster'}, \
        #{'ApplicableDuration': 'subscan'}, \
}


SDFlag_Desc = {
    "TsysFlag"               : "Outlier Tsys",
    "RmsPreFitFlag"          : "Baseline RMS pre-fit",
    "RmsPostFitFlag"         : "Baseline RMS post-fit",
    "RunMeanPreFitFlag"      : "Running mean pre-fit",
    "RunMeanPostFitFlag"     : "Running mean post-fit",
    "RmsExpectedPreFitFlag"  : "Expected RMS pre-fit",
    "RmsExpectedPostFitFlag" : "Expected RMS post-fit"
}

# ApplicableDuration: 'raster' | 'subscan'
