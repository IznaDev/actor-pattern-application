# What is about ?

I love Rust and I am convinced that it will become an essential programming language in the coming years because it is so enjoyable to work with, high-performing, and secure.

But Rust is demanding, and it expects programmers to delve deeply into the concepts in order to code at a low level and harness its full power.

This involves relearning many coding concepts. This code demonstrates one of them. 

The asynchronous programming.

For many people, “async” goes hand in hand with “mutex”. But you have an other way to run asynchonous tasks very efficiently without using mutex.

Actually, the mutex isn't efficient at all in this scenario.

# What is the scenario ?

A hub that centralizes all the data sent to it by various devices. It also sends data.

Imagine thousands of different types of data being sent all at once!

Can you handle that ? How do you handle that ?

The checker/ represents the devices. In fact, it launches an amount of tests that verify the code robustness in the device-hub/ directory.

The checker runs **8 sequential phases**:

| # | Phase | What is tested |
|---|---|---|
| 1 | Connection | Handshake of 3 devices (Controller, Light, Sensor) |
| 2 | Button → Light | Button 0 pressed → Light receives RED |
| 3 | LED Feedback | Controller receives white LED for the pressed button |
| 4 | Color change | Button 1 pressed → Light receives GREEN |
| 5 | Button released | Button 0 released → Controller receives LED off |
| 6 | Sensor data | Sensor sends a value, hub stores it |
| 7 | State query | Monitor connects, queries → coherent JSON |
| 8 | Rapid sequence | 3 buttons pressed quickly → 3 Light commands in order |

**Success criteria:** `8/8 — All tests pass!`

 Don't pay attention at the checker juste know that it your set of tests.

The device-hub/ contains the data management program, where many people would implement a "Mutex" that handle the state access. 

We propose a different approch.

# What is the approch?

In the device-hub/ directory I implemented a pattern based on channels (mpsc, oneshot, broadcast): the Actor Pattern. 

I'm not going to try to explain something that's already been explained by a programmer who's much more skilled than I am. I'll just share the link to her article, which was a lifesaver when I was trying to solve the problem my team was facing on our project. 

https://ryhl.io/blog/actors-with-tokio/

Alice is one of the people who create Tokio, the Rustaceans favourite asynchronous runtime. I think that says it all. 

Here is an example of how this pattern can be applied to a real-life problem.

# How do you get this thing started?

First, run device-hub: ```bash cd device-hub && cargo run ```

Then, run checker: ```bash cd checker && cargo run ```


# Last word

There are lots of flaws in my code,particularly in the TCP communication, but I hope to improve them. However, I’m quite proud of the pattern I’ve implemented. I think this work will be useful to some people, which is why I’m sharing it with you. 

Don't forget Rust is the future !!




