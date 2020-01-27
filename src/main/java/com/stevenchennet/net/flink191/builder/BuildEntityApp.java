package com.stevenchennet.net.flink191.builder;

public class BuildEntityApp {
    static void Test() {
        Entity entity = MyBuilder.of(Entity::new)
                .with(Entity::setId, 1)
                .with(Entity::setName, "zhangsan")
                .with(Entity::setAbc, 12L)
                .build();
    }

    static class Entity {
        private int id;
        private String name;
        private long abc;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getAbc() {
            return abc;
        }

        public void setAbc(long abc) {
            this.abc = abc;
        }


    }

}
