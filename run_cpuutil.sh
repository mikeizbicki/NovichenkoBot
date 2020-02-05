#!/bin/sh

top -bn1 | tail -n +8 | cut -b 47-53 | paste -sd+ | bc
