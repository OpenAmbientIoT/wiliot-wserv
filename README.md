# WServ

William Wood Harter building some kind of Wiliot app.

No idea what it does yet.  Maybe a fridge monitor or around the house measurements. The tracking from a distance seems hard since I don't have any kind of access to a logistics workflow.


```
docker build -t wserv1 -f Dockerfile_app .
docker run -p 5000:5000 --name wserv wserv1

# shell into the container
docker exec -it wserv bash

# send data to the flask endpoint
curl.exe -X POST -H "Content-Type: Application/json" -d "{\"hello\":\"world\"}" http://localhost:5000/api/add_data
```


### running in debug mode
I like the ability to shell into the container and start and stop flask and the other apps without having to rebuild the whole container every time.

```
docker build --build-arg WSERV_DEBUG="TRUE" -t wserv1 -f Dockerfile_app .
docker run -p 5000:5000 -p 3000:3000 -v ${pwd}:/mnt/wserv --name wserv wserv1

docker exec -it wserv bash
cd /mnt/wserv
python3 watch.py&
cd wflask
flask run --host=0.0.0.0

cd /mnt/wserv/tools
python3 pixel_simulator.py

cd /mnt/wserv/ui
npm run start

```

## React UI
The UI is a react interface that will evetually be generated into a page and hosted along with flask. For now in debug you'll run it separate.

Wiliot Hackathon entry for William Wood Harter<br/>
(c) copyright 2023 - William Wood Harter

License: MIT License
