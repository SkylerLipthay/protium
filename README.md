# Protium

Protium makes any data structure atomic and durable (see [ACID](https://en.wikipedia.org/wiki/ACID#Consistency)). The name comes from the ordinary hydrogen isotope, which is both atomic and durable (stable). Sorry, that's really the best I could do.

## To-do

* Will `FileStorage` data be corrupt if the system crashes while writing the last byte of a chunk? That is, do storage drives guarantee byte atomicity, or only bit atomicity? If byte atomicity isn't guaranteed, then a CRC32 will have to be saved along with the chunk length to provide atomicity.
