package com.mesosphere.sdk.scheduler.multi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.mesosphere.sdk.storage.MemPersister;
import com.mesosphere.sdk.storage.Persister;

public class ServiceStoreTest {

    @Test
    public void testBasicFunctionality() throws Exception {
        Persister persister = new MemPersister();
        ServiceStore store = new ServiceStore(persister);

        Assert.assertFalse(store.get("foo").isPresent());
        Assert.assertTrue(store.list().isEmpty());

        store.put("foo", "bar");

        Assert.assertEquals(Collections.singletonMap("foo", "bar"), store.list());
        Assert.assertEquals("bar", store.get("foo").get());
        Assert.assertFalse(store.get("baz").isPresent());

        store.put("baz", "bazz");

        Map<String, String> entries = new HashMap<>();
        entries.put("foo", "bar");
        entries.put("baz", "bazz");
        Assert.assertEquals(entries, store.list());
        Assert.assertEquals("bar", store.get("foo").get());
        Assert.assertEquals("bazz", store.get("baz").get());
        Assert.assertFalse(store.get("bar").isPresent());

        store.getUninstallCallback().uninstalled("foo");

        Assert.assertEquals(Collections.singletonMap("baz", "bazz"), store.list());
        Assert.assertFalse(store.get("foo").isPresent());
        Assert.assertEquals("bazz", store.get("baz").get());

        store.getUninstallCallback().uninstalled("baz");

        Assert.assertFalse(store.get("foo").isPresent());
        Assert.assertTrue(store.list().isEmpty());
    }

    @Test
    public void testSlashedName() throws Exception {
        Persister persister = new MemPersister();
        ServiceStore store = new ServiceStore(persister);

        Assert.assertFalse(store.get("/path/to/foo").isPresent());
        Assert.assertTrue(store.list().isEmpty());

        store.put("/path/to/foo", "bar");

        Assert.assertEquals(Collections.singletonMap("/path/to/foo", "bar"), store.list());
        Assert.assertEquals("bar", store.get("/path/to/foo").get());

        store.getUninstallCallback().uninstalled("/path/to/foo");

        Assert.assertFalse(store.get("/path/to/foo").isPresent());
        Assert.assertTrue(store.list().isEmpty());
    }
}
