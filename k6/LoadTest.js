import http from 'k6/http';
import { check, group, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '1m', target: 240 }, 
  ],
  thresholds: {
    http_req_duration: ['p(99)<1500'], // 99% of requests must complete below 1.5s    
  }
};

const BASE_URL = 'http://localhost:8080';

export default function () {
  const payload = JSON.stringify({
    name: 'lorem',
    surname: 'ipsum',
  });
  const headers = { 'Content-Type': 'application/json' };

  let publisherRes = http.post(BASE_URL+'/publishMessage?topicId=topic88', payload, { headers });

  console.log(publisherRes);

  check(publisherRes, {
    'Message published successfully': (resp) => resp.status === 200
  });
  sleep(1);
}